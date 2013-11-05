using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using TheFactory.Datastore.Helpers;
using System.Collections;
using System.Text;

namespace TheFactory.Datastore {

    public class Database: IDisposable {
        private ObservableCollection<ITablet> tablets;
        private List<ITablet> mutableTablets;
        private FileManager fileManager;
        private TransactionLogWriter writeLog;

        public bool IsOpened { get; private set; }

        internal Database() {
            tablets = new ObservableCollection<ITablet>();
            mutableTablets = new List<ITablet>();
            mutableTablets.Add(new MemoryTablet());
        }

        public Database(string path) : this() {
            fileManager = new FileManager(path);
        }

        public void Open() {
            if (IsOpened) {
                return;
            }
            // Load all tablet files.
            if (fileManager != null) {
                var logfile = fileManager.GetTransactionLog();
                using (var log = new TransactionLogReader(logfile)) {
                    var tablet = (MemoryTablet)mutableTablets[mutableTablets.Count - 1];
                    foreach (var transaction in log.Transactions()) {
                        tablet.Apply(new Batch(transaction));
                    }
                }

                writeLog = new TransactionLogWriter(logfile);

                foreach (var filename in fileManager.ReadTabletStackFile()) {
                    PushTablet(filename);
                }
                tablets.CollectionChanged += (sender, args) => {
                    fileManager.WriteTabletStackFile(tablets);
                };
            }
            IsOpened = true;
        }

        public void Close() {
            if (!IsOpened) {
                return;
            }
            IsOpened = false;
            while (tablets.Count > 0) {
                PopTablet();
            }

            if (writeLog != null) {
                writeLog.Dispose();
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

            var header = reader.ReadHeader(stream);
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
                // Write the header.
                writer.Write(header.Raw, 0, header.Raw.Length);

                while (!EndOfBlocks(stream) && stream.Position < stream.Length) {
                    var blockData = reader.ReadBlock(stream);

                    // Write the block as-is.
                    writer.Write(blockData.Info.Raw, 0, blockData.Info.Raw.Length);
                    writer.Write(blockData.Raw, 0, blockData.Raw.Length);

                    if (blockData.Info.Checksum != 0 && blockData.Info.Checksum != blockData.Checksum) {
                        // Bad block checksum.
                        continue;
                    }

                    if (blockData.Info.Type != BlockType.Data) {
                        // Skip non-Data blocks.
                        continue;
                    }

                    var block = new Block((Slice)blockData.Data);
                    if (callback != null) {
                        callback(block.Find());
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
            tablets.Add(new FileTablet(filename));
        }

        public void PopTablet() {
            if (tablets.Count == 0) {
                return;
            }
            var t = tablets[tablets.Count - 1];
            tablets.RemoveAt(tablets.Count - 1);
            t.Close();
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find(null);
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            var searches = tablets.Concat(mutableTablets).Reverse().ToList();

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

        public Slice Get(string key) {
            return Get((Slice)Encoding.UTF8.GetBytes(key));
        }

        public void Apply(Batch batch) {
            if (writeLog != null) {
                writeLog.EmitTransaction(batch.ToSlice());
            }

            // Last mutableTablet is the writing tablet.
            var tablet = (MemoryTablet)mutableTablets[mutableTablets.Count - 1];
            tablet.Apply(batch);
        }

        public void Put(Slice key, Slice val) {
            var batch = new Batch();
            batch.Put(key, val);
            Apply(batch);
        }

        public void Put(string key, Slice val) {
            Put((Slice)Encoding.UTF8.GetBytes(key), val);
        }

        public void Put(string key, string val) {
            Put((Slice)Encoding.UTF8.GetBytes(key), (Slice)Encoding.UTF8.GetBytes(val));
        }

        public void Delete(Slice key) {
            var batch = new Batch();
            batch.Delete(key);
            Apply(batch);
        }

        public void Delete(string key) {
            Delete((Slice)Encoding.UTF8.GetBytes(key));
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
