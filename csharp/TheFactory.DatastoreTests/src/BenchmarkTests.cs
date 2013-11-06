using System;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;
using System.Text;

namespace TheFactory.DatastoreTests {
    public class BenchmarkArgs {
        public BenchmarkArgs() {
        }

        // write key/value pairs sequentially
        public bool Seq = true;

        // number of key/value pairs to write
        public int Count = 1000000;

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
            args.Count = 1000;

            RunBenchmark("FillSeq", args);
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
    }
}

