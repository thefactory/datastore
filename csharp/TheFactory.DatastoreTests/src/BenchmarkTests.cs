using System;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;
using System.Text;
using System.Collections.Generic;
using Splat;
using TheFactory.FileSystem;
using TheFactory.FileSystem.IOS;

namespace TheFactory.DatastoreTests {
    public class BenchmarkArgs {
        public BenchmarkArgs() {
        }

        // write key/value pairs sequentially
        public bool Seq = true;

        // number of key/value pairs to write
        public int Count = 100000;

        // key/value entries per batch
        public int EntriesPerBatch = 1;

        public int ValueLen = 100;
    }

    // benchmark / scale tests for datastore
    [TestFixture, Explicit]
    public class BenchmarkTests {
        String tmpDir;

        [SetUp]
        public void setUp() {
            Locator.CurrentMutable.RegisterConstant(new IOSFileSystem(), typeof(IFileSystem));
            tmpDir = Path.Combine(Path.GetTempPath(), "benchmark" + Utils.RandomString(4));
            Directory.CreateDirectory(tmpDir);
        }

        [TearDown]
        public void tearDown() {
            Directory.Delete(tmpDir, true);
            tmpDir = null;
        }

        void RunBenchmark(String name, BenchmarkArgs args) {
            var db = Database.Open(tmpDir) as Database;

            var stats = new Stats();
            stats.Start();
            DoWrite(db, args, stats);
            stats.Finish();

            Console.WriteLine(Header(name, args) + stats.Report(name));
        }

        public String Header(String name, BenchmarkArgs args) {
            var buf = new StringWriter();
            buf.Write("{0} args:\n", name);
            buf.Write("Keys:     {0} bytes each. sequential: {1}\n", 16, args.Seq);
            buf.Write("Values:   {0} bytes each\n", args.ValueLen);
            buf.Write("Entries:  {0}. batches of {1}\n", args.Count, args.EntriesPerBatch);
            buf.Write("-------------------------------------------------------------------\n");

            return buf.ToString();
        }

        [Test]
        public void DbFillSeq() {
            var args = new BenchmarkArgs();
            RunBenchmark("DbFillSeq", args);
        }

        [Test]
        public void DbFillRandom() {
            var args = new BenchmarkArgs();
            args.Seq = false;
            RunBenchmark("DbFillRandom", args);
        }

        [Test]
        public void DbFindAll() {
            var name = "DbFindAll";
            var args = new BenchmarkArgs();

            // fill the database but throw away the writing stats
            var db = Database.Open(tmpDir) as Database;
            DoWrite(db, args, new Stats());

            var stats = new Stats();
            stats.Start();
            foreach (var kv in db.Find()) {
                stats.AddBytes(kv.Key.Length + kv.Value.Length);
                stats.FinishedSingleOp();
            }
            stats.Finish();

            Report(name, args, stats);
        }

        [Test]
        public void DbReadSeq() {
            var name = "DbReadSeq";
            var args = new BenchmarkArgs();

            // fill the database but throw away the writing stats
            var db = Database.Open(tmpDir) as Database;
            DoWrite(db, args, new Stats());

            var stats = new Stats();
            stats.Start();
            DoRead(db, args, stats);
            stats.Finish();

            Report(name, args, stats);
        }

        [Test]
        public void DbReadRandom() {
            var name = "DbReadRandom";
            var args = new BenchmarkArgs();

            // fill the database but throw away the writing stats
            var db = Database.Open(tmpDir) as Database;
            DoWrite(db, args, new Stats());

            var stats = new Stats();
            stats.Start();
            DoRead(db, args, stats);
            stats.Finish();

            Report(name, args, stats);
        }

        [Test]
        public void TabletFillSeq() {
            var name = "TabletFillSeq";
            var args = new BenchmarkArgs();
            var filename = Path.Combine(tmpDir, "tablet.tab");

            var stats = new Stats();
            stats.Start();
            WriteTablet(filename, StatsWrapper(KVStream(args), stats), new TabletWriterOptions());
            stats.Finish();

            Report(name, args, stats);
        }

        public void WriteTablet(string filename, IEnumerable<IKeyValuePair> kvs, TabletWriterOptions opts) {
            var writer = new TabletWriter();
            using (var output = new BinaryWriter(File.OpenWrite(filename))) {
                writer.WriteTablet(output, kvs, opts);
            }
        }

        [Test]
        public void TabletFindAll() {
            var name = "TabletFindAll";
            var args = new BenchmarkArgs();
            var filename = Path.Combine(tmpDir, "tablet.tab");

            WriteTablet(filename, KVStream(args), new TabletWriterOptions());

            var reader = new FileTablet(filename, new TabletReaderOptions());

            var stats = new Stats();
            stats.Start();
            foreach (var kv in StatsWrapper(reader.Find(), stats)) {
                // nop: StatsWrapper is tracking our work
            }
            stats.Finish();

            Report(name, args, stats);
        }

        [Test]
        public void TabletReadSeq() {
            var name = "TabletReadSeq";
            var args = new BenchmarkArgs();
            args.Count = 10000; // single-key tablet read is slow for now

            var filename = Path.Combine(tmpDir, "tablet.tab");

            WriteTablet(filename, KVStream(args), new TabletWriterOptions());

            var reader = new FileTablet(filename, new TabletReaderOptions());
            var stats = new Stats();

            stats.Start();
            DoRead(reader, args, stats);
            stats.Finish();

            Report(name, args, stats);
        }

        [Test]
        public void TabletReadRandom() {
            var name = "TabletReadRandom";
            var args = new BenchmarkArgs();
            args.Count = 10000; // single-key tablet read is slow for now

            var filename = Path.Combine(tmpDir, "tablet.tab");

            WriteTablet(filename, KVStream(args), new TabletWriterOptions());

            var reader = new FileTablet(filename, new TabletReaderOptions());
            var stats = new Stats();

            // read keys in random order
            args.Seq = false;

            stats.Start();
            DoRead(reader, args, stats);
            stats.Finish();

            Report(name, args, stats);
        }

        [Test]
        public void ReplayTransactionLog() {
            var name = "ReplayTransactionLog";
            var args = new BenchmarkArgs();
            var options = new Options();

            using (var db = (Database)Database.Open(tmpDir)) {
                DoWrite(db, args, new Stats());
            }

            // manually replay the transaction log for stats gathering
            var path = Path.Combine(tmpDir, "write.log");
            var tablet = new MemoryTablet();
            var stats = new Stats();

            stats.Start();
            using (var stream = Locator.Current.GetService<IFileSystem>().GetStream(path, TheFactory.FileSystem.FileMode.Open, TheFactory.FileSystem.FileAccess.Read))
            using (var log = new TransactionLogReader(stream)) {
                foreach (var transaction in log.Transactions()) {
                    tablet.Apply(new Batch(transaction));
                    stats.AddBytes(transaction.Length);
                    stats.FinishedSingleOp();
                }
            }
            stats.Finish();

            Report(name, args, stats);
        }

        public void Report(string name, BenchmarkArgs args, Stats stats) {
            Console.WriteLine(Header(name, args) + stats.Report(name));
        }

        public IEnumerable<IKeyValuePair> StatsWrapper(IEnumerable<IKeyValuePair> kvs, Stats stats) {
            long bytes = 0;
            foreach (var kv in kvs) {
                yield return kv;

                bytes += kv.Key.Length + kv.Value.Length;
                stats.FinishedSingleOp();
            }
            stats.AddBytes(bytes);

            yield break;
        }

        public IEnumerable<IKeyValuePair> KVStream(BenchmarkArgs args) {
            var rand = new Random();
            var enc = new ASCIIEncoding();

            Pair pair = new Pair();

            for (int i=0; i<args.Count; i++) {
                int k = args.Seq ? i : rand.Next(args.Count);
                var key = enc.GetBytes(String.Format("{0:d16}", k));
                var val = enc.GetBytes(Utils.RandomString(args.ValueLen));

                pair.Key = (Slice)key;
                pair.Value = (Slice)val;

                yield return pair;
            }

            yield break;
        }

        public void DoWrite(Database db, BenchmarkArgs args, Stats stats) {
            var batch = new Batch();

            stats.AddMessage(String.Format("({0:d} ops)", args.Count));

            var kvs = KVStream(args).GetEnumerator();

            long bytes = 0;
            for (int i=0; i<args.Count; i += args.EntriesPerBatch) {
                batch.Clear();
                for (int j=0; j<args.EntriesPerBatch; j++) {
                    if (!kvs.MoveNext()) {
                        Assert.Fail("Unexpected short iterator in DoWrite");
                    }

                    var key = kvs.Current.Key;
                    var val = kvs.Current.Value;

                    batch.Put(key, val);
                    bytes += key.Length + val.Length;

                    stats.FinishedSingleOp();
                }

                db.Apply(batch);
            }

            stats.AddBytes(bytes);
        }

        private void DoRead(Database db, BenchmarkArgs args, Stats stats) {
            Random rand = new Random();
            for (int i = 0; i < args.Count; i++) {
                int num = args.Seq ? i : rand.Next(args.Count);
                var key = String.Format("{0:d16}", num);
                try {
                    var val = db.Get(key);
                    stats.AddBytes(val.Length);
                } catch (KeyNotFoundException) {
                }
                stats.FinishedSingleOp();
            }
        }

        private void DoRead(ITablet tab, BenchmarkArgs args, Stats stats) {
            var enc = new ASCIIEncoding();
            Random rand = new Random();

            long bytes = 0;
            for (int i = 0; i < args.Count; i++) {
                int num = args.Seq ? i : rand.Next(args.Count);
                var keyStr = String.Format("{0:d16}", num);
                var key = (Slice)enc.GetBytes(keyStr);

                var iter = tab.Find(key).GetEnumerator();
                iter.MoveNext();
                bytes += iter.Current.Value.Length;
                stats.FinishedSingleOp();
            }
            stats.AddBytes(bytes);
        }
    }
}

