using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using TheFactory.Datastore.Helpers;
using System.Collections;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Splat;
using TheFactory.FileSystem;

namespace TheFactory.Datastore {
    public class Options {
        public bool CreateIfMissing { get; set; }
        public bool DeleteOnClose { get; set; }
        public bool VerifyChecksums { get; set; }
        public int MaxMemoryTabletSize { get; set; }
        public TabletReaderOptions ReaderOptions { get; set; }
        public TabletWriterOptions WriterOptions { get; set; }

        public Options() {
            CreateIfMissing = true;
            DeleteOnClose = false;
            VerifyChecksums = false;
            MaxMemoryTabletSize = 1024 * 1024 * 4; /* 4MB default */
            ReaderOptions = new TabletReaderOptions();
            WriterOptions = new TabletWriterOptions();
        }

        public override string ToString() {
            return String.Format(
                "CreateIfMissing = {0}\n" +
                "DeleteOnClose = {1}\n" +
                "VerifyChecksums = {2}\n" +
                "MaxMemoryTabletSize = {3}\n" +
                "ReaderOptions:\n{4}\n" +
                "WriterOptions:\n{5}\n",
                CreateIfMissing, DeleteOnClose, VerifyChecksums, MaxMemoryTabletSize,
                ReaderOptions.ToString(), WriterOptions.ToString());
        }
    }

    public class KeyValueChangedEventArgs: EventArgs {
        public IKeyValuePair Pair { get; private set; }

        public KeyValueChangedEventArgs(IKeyValuePair kv) {
            Pair = kv;
        }
    }

    public class Database: IDatabase, IEnableLogger {
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
        private MemoryTablet mem = null;
        private MemoryTablet mem2 = null;
        private List<ITablet> tablets = new List<ITablet>();

        private ManualResetEventSlim canCompact = new ManualResetEventSlim(true);

        // memLock protects access to the mutable memory tablet. tabLock protects
        // access to the tablets list.
        private object memLock = new object();
        private object tabLock = new object();

        private FileManager fileManager;
        private TransactionLogWriter writeLog;

        IDisposable fsLock;

        public event EventHandler<KeyValueChangedEventArgs> KeyValueChangedEvent;

        internal Database(string path, Options opts) {
            this.fileManager = new FileManager(path);
            this.opts = opts;
            this.fs = Locator.Current.GetService<IFileSystem>();
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
                    throw new InvalidOperationException(String.Format("Directory `{0}` must exist", fileManager.Dir));
                }
                fs.CreateDirectory(fileManager.Dir);
            }

            fsLock = fs.FileLock(fileManager.GetLockFile());

            LoadMemoryTablets();

            var tabletStack = fileManager.GetTabletStack();
            if (fs.Exists(tabletStack)) {
                using (var file = fs.GetStream(tabletStack, FileMode.Open, FileAccess.Read)) {
                    var reader = new StreamReader(file);
                    while (!reader.EndOfStream) {
                        var filename = fileManager.DbFilename(reader.ReadLine());
                        tablets.Add(new FileTablet(filename, opts.ReaderOptions));
                    }
                }
            }
        }

        void LoadMemoryTablets() {
            var transLog = fileManager.GetTransactionLog();
            var immTransLog = fileManager.GetImmutableTransactionLog();

            mem = new MemoryTablet();
            if (fs.Exists(transLog)) {
                ReplayTransactions(mem, transLog);
            }

            // If we crashed while freezing a memory tablet, freeze it again. Block
            // database loading until it completes.
            if (fs.Exists(immTransLog)) {
                var tab = new MemoryTablet();
                ReplayTransactions(tab, immTransLog);

                // Block until the immutable memory tablet has been frozen.
                Compact(tab);
                fs.Remove(immTransLog);
            }

            writeLog = new TransactionLogWriter(fs.GetStream(transLog, FileMode.Append, FileAccess.Write, FileShare.None));
        }

        void ReplayTransactions(MemoryTablet tablet, string path) {
            using (var log = new TransactionLogReader(fs.GetStream(path, FileMode.Open, FileAccess.Read))) {
                foreach (var transaction in log.Transactions()) {
                    tablet.Apply(new Batch(transaction));
                }
            }
        }

        public void Close() {
            if (writeLog != null) {
                writeLog.Dispose();
                writeLog = null;
            }

            // Wait until any compaction is finished.
            canCompact.Wait();

            if (fsLock != null) {
                fsLock.Dispose();
                fsLock = null;
            }

            if (opts.DeleteOnClose) {
                fs.RemoveDirectory(fileManager.Dir);
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

        public void PushTablet(string filename) {
            lock (tabLock) {
                tablets.Add(new FileTablet(filename, opts.ReaderOptions));
                WriteLevel0List();
            }
        }

        void WriteLevel0List() {
            // tabLock must be held to call WriteLevel0List
            using (var stream = fs.GetStream(fileManager.GetTabletStack(), FileMode.Create, FileAccess.Write, FileShare.None)) {
                var writer = new StreamWriter(stream);
                foreach (var t in tablets) {
                    writer.WriteLine(Path.GetFileName(t.Filename));
                }
                writer.Dispose();
            }
        }

        void MaybeCompactMem() {
            // This method locks the tablets briefly to determine whether mem is due for compaction.
            //
            // If mem is not due for compaction, it returns after that.
            // If it is due and no compaction is running, it fires off a background compaction task.
            // If a compaction is running, it blocks until that completes and checks again.
            //
            // Use the "canCompact" ManualResetEventSlim as a condition variable to accomplish the above.

            Func<bool> memFull = () => (mem.ApproxSize > opts.MaxMemoryTabletSize);

            bool shouldCompact;
            lock (tabLock) {
                shouldCompact = memFull();
            }

            if (shouldCompact) {
                canCompact.Wait();

                lock(tabLock) {
                    if (memFull()) {
                        canCompact.Reset();
                        CompactMem();
                    }
                }
            }
        }

        void CompactMem() {
            var transLog = fileManager.GetTransactionLog();
            var immTransLog = fileManager.GetImmutableTransactionLog();

            // Move the current writable memory tablet to mem2.
            lock (tabLock) {
                mem2 = mem;
                mem = new MemoryTablet();

                writeLog.Dispose();
                fs.Move(transLog, immTransLog);
                writeLog = new TransactionLogWriter(fs.GetStream(transLog, FileMode.Append, FileAccess.Write));

                Task.Run(() => {
                    try {
                        Compact(mem2);
                        fs.Remove(immTransLog);
                        mem2 = null;
                    } catch (Exception e) {
                        this.Log().ErrorException("Exception in tablet compaction", e);
                    } finally {
                        // Set canCompact to avoid deadlock, but at this point we'll
                        // try and fail to compact mem2 on every database write.
                        canCompact.Set();
                    }
                });
            }
        }

        void Compact(MemoryTablet tab) {
            var tabfile = fileManager.DbFilename(String.Format("{0}.tab", System.Guid.NewGuid().ToString()));
            var writer = new TabletWriter();

            using (var stream = fs.GetStream(tabfile, FileMode.Create, FileAccess.Write))
            using (var output = new BinaryWriter(stream)) {
                writer.WriteTablet(output, tab.Find(Slice.Empty), opts.WriterOptions);
            }

            PushTablet(tabfile);
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

            return null;
        }

        internal void Apply(Batch batch) {
            lock (memLock) {
                writeLog.EmitTransaction(batch.ToSlice());
                mem.Apply(batch);
                MaybeCompactMem();
            }

            if (KeyValueChangedEvent != null) {
                foreach (var kv in batch.Pairs()) {
                    KeyValueChangedEvent(this, new KeyValueChangedEventArgs(kv));
                }
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
