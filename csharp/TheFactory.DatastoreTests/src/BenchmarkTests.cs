using System;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;
using System.Text;
using System.Collections.Generic;

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
    [TestFixture]
    public class BenchmarkTests {
        String tmpDir;

        [SetUp]
        public void setUp() {
            tmpDir = Path.Combine(Path.GetTempPath(), "benchmark" + Utils.RandomString(4));
            Directory.CreateDirectory(tmpDir);
        }

        [TearDown]
        public void tearDown() {
            Directory.Delete(tmpDir, true);
            tmpDir = null;
        }

        void RunBenchmark(String name, BenchmarkArgs args) {
            var db = Database.Open(tmpDir);

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
        public void FillSeq() {
            var args = new BenchmarkArgs();
            RunBenchmark("FillSeq", args);
        }

        [Test]
        public void FillRandom() {
            var args = new BenchmarkArgs();
            args.Seq = false;
            RunBenchmark("FillRandom", args);
        }

        [Test]
        public void FindAll() {
            var name = "FindAll";
            var args = new BenchmarkArgs();

            // fill the database but throw away the writing stats
            var db = Database.Open(tmpDir);
            DoWrite(db, args, new Stats());

            var stats = new Stats();
            stats.Start();
            foreach (var kv in db.Find()) {
                stats.AddBytes(kv.Key.Length + kv.Value.Length);
                stats.FinishedSingleOp();
            }
            stats.Finish();

            Console.WriteLine(Header(name, args) + stats.Report(name));
        }

        [Test]
        public void ReadSeq() {
            var name = "ReadSeq";
            var args = new BenchmarkArgs();

            // fill the database but throw away the writing stats
            var db = Database.Open(tmpDir);
            DoWrite(db, args, new Stats());

            var stats = new Stats();
            stats.Start();
            DoRead(db, args, stats);
            stats.Finish();

            Console.WriteLine(Header(name, args) + stats.Report(name));
        }

        [Test]
        public void ReadRandom() {
            var name = "ReadRandom";
            var args = new BenchmarkArgs();

            // fill the database but throw away the writing stats
            var db = Database.Open(tmpDir);
            DoWrite(db, args, new Stats());

            var stats = new Stats();
            stats.Start();
            DoRead(db, args, stats);
            stats.Finish();

            Console.WriteLine(Header(name, args) + stats.Report(name));
        }

        [Test]
        public void ReplayTransactionLog() {
            var name = "ReplayTransactionLog";
            var args = new BenchmarkArgs();
            var options = new Options();

            using (var db = Database.Open(tmpDir)) {
                DoWrite(db, args, new Stats());
            }

            // manually replay the transaction log for stats gathering
            var path = Path.Combine(tmpDir, "write.log");
            var tablet = new MemoryTablet();
            var stats = new Stats();

            stats.Start();
            using (var log = new TransactionLogReader(options.FileSystem.Open(path))) {
                foreach (var transaction in log.Transactions()) {
                    tablet.Apply(new Batch(transaction));
                    stats.AddBytes(transaction.Length);
                    stats.FinishedSingleOp();
                }
            }
            stats.Finish();

            Console.WriteLine(Header(name, args) + stats.Report(name));
        }

        public void DoWrite(Database db, BenchmarkArgs args, Stats stats) {
            var batch = new Batch();
            var rand = new Random();
            var enc = new ASCIIEncoding();

            stats.AddMessage(String.Format("({0:d} ops)", args.Count));

            long bytes = 0;
            for (int i=0; i<args.Count; i += args.EntriesPerBatch) {
                batch.Clear();
                for (int j=0; j<args.EntriesPerBatch; j++) {
                    int k = args.Seq ? i + j : rand.Next(args.Count);

                    var key = enc.GetBytes(String.Format("{0:d16}", k));
                    var val = enc.GetBytes(Utils.RandomString(args.ValueLen));

                    batch.Put((Slice)key, (Slice)val);

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
                int num = rand.Next(args.Count);
                var key = String.Format("{0:d16}", num);
                try {
                    var val = db.Get(key);
                    stats.AddBytes(val.Length);
                } catch (KeyNotFoundException e) {
                }
                stats.FinishedSingleOp();
            }
        }
    }
}

