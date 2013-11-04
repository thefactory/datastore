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

        [Test]
        public void FillSeq() {
            var db = new Database(tmpDir);
            db.Open();

            var args = new BenchmarkArgs();
            args.Count = 1000;

            DoWrite(db, args);
        }

        public void DoWrite(Database db, BenchmarkArgs args) {
            var batch = new Batch();
            var rand = new Random();
            var enc = new ASCIIEncoding();

            for (int i=0; i<args.Count; i += args.EntriesPerBatch) {
                batch.Clear();
                for (int j=0; j<args.EntriesPerBatch; j++) {
                    int k = args.Seq ? i + j : rand.Next(args.Count);
                    var key = enc.GetBytes(String.Format("{0:d16}", k));
                    var val = enc.GetBytes(Utils.RandomString(args.ValueLen));

                    batch.Put((Slice)key, (Slice)val);
                }

                db.Apply(batch);
            }
        }
    }
}

