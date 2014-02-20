using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using TheFactory.Datastore.Helpers;
using System.Collections;
using System.Text;

namespace TheFactory.Datastore {
    public class Options {
        public bool CreateIfMissing { get; set; }
        public IFileSystem FileSystem { get; set; }
        public bool VerifyChecksums { get; set; }

        public Options() {
            CreateIfMissing = true;
            FileSystem = new FileSystem();
            VerifyChecksums = false;
        }
    }

    public class Database: IDatabase {
        private Options opts;
        private IFileSystem fs;

        // Database provides a unified key-value view across three things:
        //   a mutable in-memory tablet (writes go here)
        //   an immutable in-memory tablet (temporary; while being written to disk)
        //   a collection of immutable file tablets
        //
        // The immutable file tablets are treated like the 0th level of leveldb:
        //   they can contain overlapping keys
        //   in case of duplicate keys, the most recently written tablet wins
        //
        // Changes to any of these references are protected with tabLock.
        private MemoryTablet mem = new MemoryTablet();
        private MemoryTablet mem2 = null;
        private List<ITablet> tablets = new List<ITablet>();
        private object tabLock = new object();

        private FileManager fileManager;
        private TransactionLogWriter writeLog;

        IDisposable fsLock;
        private object batchLock = new object();

        internal Database(string path, Options opts) {
            this.fileManager = new FileManager(path);
            this.opts = opts;
            this.fs = opts.FileSystem;
        }

        public static IDatabase Open(string path) {
            return Open(path, new Options());
        }

        public static IDatabase Open(string path, Options opts) {
            var db = new Database(path, opts);
            db.Open();
            return db;
        }

        private void Open() {
            if (!fs.Exists(fileManager.Dir)) {
                if (!opts.CreateIfMissing) {
                    throw new DirectoryNotFoundException();
                }
                fs.Mkdirs(fileManager.Dir);
            }

            fsLock = fs.Lock(fileManager.GetLockFile());

            var transLog = fileManager.GetTransactionLog();
            if (fs.Exists(transLog)) {
                ReplayTransactions(transLog);
            }

            writeLog = new TransactionLogWriter(fs.Append(transLog));

            var tabletStack = fileManager.GetTabletStack();
            if (fs.Exists(tabletStack)) {
                var reader = new StreamReader(fs.Open(tabletStack));
                while (!reader.EndOfStream) {
                    PushTablet(reader.ReadLine());
                }
            }
        }

        void ReplayTransactions(string path) {
            using (var log = new TransactionLogReader(fs.Open(path))) {
                foreach (var transaction in log.Transactions()) {
                    mem.Apply(new Batch(transaction));
                }
            }
        }

        public void Close() {
            if (writeLog != null) {
                writeLog.Dispose();
                writeLog = null;
            }

            if (fsLock != null) {
                fsLock.Dispose();
                fsLock = null;
            }
        }

        public void Dispose() {
            Close();
        }

        private bool EndOfBlocks(Stream stream) {
            var peek = stream.ReadInt();
            stream.Seek(-4, SeekOrigin.Current);
            // The meta index block signals the end of blocks.
            return peek == Constants.MetaIndexMagic;
        }

        public void PushTabletStream(Stream stream, Action<IEnumerable<IKeyValuePair>> callback) {
            PushTabletStream(stream, System.Guid.NewGuid().ToString(), callback);
        }

        internal void PushTabletStream(Stream stream, string filename, Action<IEnumerable<IKeyValuePair>> callback) {
            var reader = new TabletReader();

            var headerData = (Slice)stream.ReadBytes(Constants.TabletHeaderLength);
            var header = reader.ParseHeader(headerData);
            if (header.Magic != Constants.TabletMagic) {
                // Not a tablet.
                throw new TabletValidationException("bad magic");
            }

            if (header.Version < 1) {
                throw new TabletValidationException("bad version: " + header.Version);
            }

            var filepath = Path.Combine(fileManager.Dir, filename);
            var fs = new FileStream(filepath, FileMode.Create, FileAccess.Write);

            using (var writer = new BinaryWriter(fs)) {
                writer.Write(headerData.Array, headerData.Offset, headerData.Length);

                while (!EndOfBlocks(stream) && stream.Position < stream.Length) {
                    var headerOffset = stream.Position;
                    var dataLength = reader.ReadBlockHeaderLength(stream);
                    var headerLength = stream.Position - headerOffset;

                    // Reset the stream position and read the whole block in one shot.
                    stream.Position = headerOffset;

                    var blockData = (Slice)stream.ReadBytes((int)(headerLength + dataLength));

                    // Write the block as-is.
                    writer.Write(blockData.Array, blockData.Offset, blockData.Length);

                    var block = new TabletBlock(blockData);

                    if (!block.IsChecksumValid) {
                        // Skip blocks with bad checksums.
                        continue;
                    }

                    if (block.Type != BlockType.Data) {
                        // Skip non-Data blocks.
                        continue;
                    }

                    var blockReader = new BlockReader(block.KvData);
                    if (callback != null) {
                        callback(blockReader.Find());
                    }
                }

                // Read/write the remainder in 4KiB chunks.
                var size = 4096;
                var buf = new byte[size];
                int read = 0;
                while ((read = stream.Read(buf, 0, size)) != 0) {
                    writer.Write(buf, 0, read);
                }
            }

            PushTablet(filepath);
        }

        public void PushTablet(string filename) {
            lock (tabLock) {
                tablets.Add(new FileTablet(filename, new TabletReaderOptions()));
                WriteLevel0List();
            }
        }

        void WriteLevel0List() {
            using (var stream = fs.Create(fileManager.GetTabletStack())) {
                var writer = new StreamWriter(stream);
                foreach (var t in tablets) {
                    if (t.Filename != null) {
                        writer.WriteLine(t.Filename.Trim());
                    }
                }
            }
        }

        ITablet[] CurrentTablets() {
            // Return all a snapshot of current tablets in order of search priority.
            var all = new List<ITablet>(tablets.Count + 2);

            lock(tabLock) {
                all.Add(mem);
                if (mem2 != null) {
                    all.Add(mem2);
                }

                for (var i = tablets.Count - 1; i >= 0; i--) {
                    all.Add(tablets[i]);
                }
            }

            return all.ToArray();
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            var searches = CurrentTablets();
            var iter = new ParallelEnumerator(searches.Count(), (i) => {
                return searches[i].Find(term).GetEnumerator();
            });

            using (iter) {
                while (iter.MoveNext()) {
                    if (!iter.Current.IsDeleted) {
                        yield return iter.Current;
                    }
                }
            }

            yield break;
        }

        public Slice Get(Slice key) {
            foreach (var p in Find(key)) {
                if (Slice.Compare(p.Key, key) == 0) {
                    return p.Value.Detach();
                } else {
                    break;
                }
            }

            throw new KeyNotFoundException(key.ToUTF8String());
        }

        internal void Apply(Batch batch) {
            lock (batchLock) {
                writeLog.EmitTransaction(batch.ToSlice());
                mem.Apply(batch);
            }
        }

        public void Put(Slice key, Slice val) {
            var batch = new Batch();
            batch.Put(key, val);
            Apply(batch);
        }

        public void Delete(Slice key) {
            var batch = new Batch();
            batch.Delete(key);
            Apply(batch);
        }

        private class ParallelEnumerator: IEnumerator<IKeyValuePair> {
            SortedSet<QueuePair> queue;
            List<IEnumerator<IKeyValuePair>> iters;
            Pair current;

            public ParallelEnumerator(int n, Func<int, IEnumerator<IKeyValuePair>> func) {
                queue = new SortedSet<QueuePair>(new PriorityComparer());
                iters = new List<IEnumerator<IKeyValuePair>>(n);

                for (int i = 0; i < n; i++) {
                    var iter = func(i);
                    if (iter.MoveNext()) {
                        queue.Add(new QueuePair(-i, iter.Current));
                    }
                    iters.Add(iter);
                }

                current = new Pair();
            }

            object IEnumerator.Current { get { return current; } }

            public IKeyValuePair Current { get { return current; } }

            public bool MoveNext() {
                if (queue.Count == 0) {
                    current = null;
                    return false;
                }

                current = Pop();

                while (queue.Count > 0 && current.Key.Equals(queue.Min.kv.Key)) {
                    // skip any items in other iterators that have the same key
                    Pop();
                }

                return true;
            }

            private Pair Pop() {
                var cur = queue.Min;
                queue.Remove(cur);

                // set up new references to this item, since we're about to advance its iterator below
                var ret = new Pair();
                ret.Key = cur.kv.Key;
                ret.Value = cur.kv.Value;
                ret.IsDeleted = cur.kv.IsDeleted;

                var iter = iters[-cur.Priority];
                if (iter.MoveNext()) {
                    queue.Add(new QueuePair(cur.Priority, iter.Current));
                }

                return ret;
            }

            public void Dispose() {
                foreach (var iter in iters) {
                    iter.Dispose();
                }
            }
            // from IEnumerator: it's ok not to support this
            public void Reset() {
                throw new NotSupportedException();
            }

            private class QueuePair {
                public int Priority { get; set; }

                public IKeyValuePair kv { get; set; }

                public QueuePair(int priority, IKeyValuePair kv) {
                    this.Priority = priority;
                    this.kv = kv;
                }
            }

            private class PriorityComparer : IComparer<QueuePair>, IComparer {
                public int Compare(Object x, Object y) {
                    return this.Compare((QueuePair)x, (QueuePair)y);
                }

                public int Compare(QueuePair x, QueuePair y) {
                    if (ReferenceEquals(x, y)) {
                        return 0;
                    }

                    var cmp = Slice.Compare(x.kv.Key, y.kv.Key);
                    if (cmp == 0) {
                        // Key is the same, the higher priority pair wins
                        return y.Priority - x.Priority;
                    }
                    return cmp;
                }
            }
        }
    }
}
