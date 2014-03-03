using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using TheFactory.Datastore;
using System.Threading;
using Splat;
using TheFactory.FileSystem;

namespace TheFactory.DatastoreTests {
    [TestFixture]
    public class DatabaseTests {
        private Database db;

        [SetUp]
        public void SetUp() {
            var opts = new Options();

            var dbPath = Path.Combine(Path.GetTempPath(), "db");
            if (Directory.Exists(dbPath)) {
                Directory.Delete(dbPath, true);
            }

            db = Database.Open(dbPath, opts) as Database;
        }

        [TearDown]
        public void TearDown() {
            db.Close();
        }

        [Test]
        public void TestCreateIfMissing() {
            var opts = new Options();
            opts.CreateIfMissing = false;

            var path = Path.Combine(Path.GetTempPath(), "does-not-exist");

            Assert.Throws(typeof(InvalidOperationException),
                          delegate { Database.Open(path, opts); });
        }

        [Test]
        public void TestFilesystemLock() {
            var path = Path.Combine(Path.GetTempPath(), "testdb" + Utils.RandomString(8));

            try {
                using (var db1 = Database.Open(path)) {
                    // attempt to reopen the database
                    Assert.Throws(typeof(IOException), delegate {
                        Database.Open(path);
                    });
                }
            } finally {
                Directory.Delete(path, true);
            }
        }

        [Test]
        public void TestMemoryLock() {
            var path = Path.Combine(Path.GetTempPath(), "testdb" + Utils.RandomString(8));
            var opts = new Options();

            using (var db1 = Database.Open(path, opts)) {
                // attempt to reopen the database
                Assert.Throws(typeof(IOException), delegate {
                    Database.Open(path, opts);
                });
            }
        }

        [Test]
        public void TestDatabaseOneFileTabletFindAll() {
            var enc = new UTF8Encoding();
            db.PushTablet("test-data/ngrams1/ngrams1-Nblock-compressed.tab");
            using (var data = new StreamReader("test-data/ngrams1/ngrams1.txt")) {
                foreach (var p in db.Find()) {
                    var kv = data.ReadLine().Split(new char[] {' '});
                    var k = enc.GetBytes(kv[0]);
                    var v = enc.GetBytes(kv[1]);
                    Assert.True(p.Key.Equals((Slice)k));
                    Assert.True(p.Value.Equals((Slice)v));
                }
                Assert.True(data.ReadLine() == null);
            }
        }

        [Test]
        public void TestDatabaseMultiFileTabletFindAll() {
            var enc = new UTF8Encoding();
            db.PushTablet("test-data/ngrams2/ngrams.tab.0");
            db.PushTablet("test-data/ngrams2/ngrams.tab.1");
            using (var data = new StreamReader("test-data/ngrams2/ngrams2.txt")) {
                foreach (var p in db.Find()) {
                    var kv = data.ReadLine().Split(new char[] {' '});
                    var k = enc.GetBytes(kv[0]);
                    var v = enc.GetBytes(kv[1]);
                    Assert.True(p.Key.Equals((Slice)k));
                    Assert.True(p.Value.Equals((Slice)v));
                }
                Assert.True(data.ReadLine() == null);
            }
        }

        [Test]
        public void TestDatabaseMultiFileTabletFindFromN() {
            var enc = new UTF8Encoding();
            db.PushTablet("test-data/ngrams2/ngrams.tab.0");
            db.PushTablet("test-data/ngrams2/ngrams.tab.1");
            using (var data = new StreamReader("test-data/ngrams2/ngrams2.txt")) {
                var count = 0;
                string[] kv;
                do {
                    count += 1;
                    kv = data.ReadLine().Split(new char[] {' '});
                } while (count < 10);  // Skip some lines to find a term.
                var term = (Slice)enc.GetBytes(kv[0]);
                foreach (var p in db.Find(term)) {
                    var k = enc.GetBytes(kv[0]);
                    var v = enc.GetBytes(kv[1]);
                    Assert.True(p.Key.Equals((Slice)k));
                    Assert.True(p.Value.Equals((Slice)v));
                    var line = data.ReadLine();
                    if (line == null) {
                        break;
                    }
                    kv = line.Split(new char[] {' '});
                }
                Assert.True(data.ReadLine() == null);
            }
        }

        [Test]
        public void TestDatabaseMultiFileTabletGetHit() {
            var enc = new UTF8Encoding();
            db.PushTablet("test-data/ngrams2/ngrams.tab.0");
            db.PushTablet("test-data/ngrams2/ngrams.tab.1");
            using (var data = new StreamReader("test-data/ngrams2/ngrams2.txt")) {
                // Just get the first key's value.
                var kv = data.ReadLine().Split(new char[] {' '});
                var k = enc.GetBytes(kv[0]);
                var v = enc.GetBytes(kv[1]);
                var result = db.Get((Slice)k);
                Assert.True(((Slice)result).Equals((Slice)v));
            }
        }

        [Test]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestDatabaseMultiFileTabletGetMiss() {
            db.PushTablet("test-data/ngrams2/ngrams.tab.0");
            db.PushTablet("test-data/ngrams2/ngrams.tab.1");
            var keyString = "Key which does not exist";
            var k = Encoding.UTF8.GetBytes(keyString);
            db.Get((Slice)k);
            Assert.True(false);
        }

        [Test]
        public void TestDatabaseMemoryOnlyWrite() {
            var k = Encoding.UTF8.GetBytes("key");
            var v = Encoding.UTF8.GetBytes("value");
            db.Put((Slice)k, (Slice)v);
            var val = db.Get((Slice)k);
            Assert.True(((Slice)val).Equals((Slice)v));
        }

        [Test]
        public void TestDatabaseMemoryOnlyReWrite() {
            var k = Encoding.UTF8.GetBytes("key");
            var v = Encoding.UTF8.GetBytes("value");
            db.Put((Slice)k, (Slice)Encoding.UTF8.GetBytes("initial value"));
            db.Put((Slice)k, (Slice)v);
            var val = db.Get((Slice)k);
            Assert.True(((Slice)val).Equals((Slice)v));
        }

        [Test]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestDatabaseMemoryOnlyDelete() {
            var k = Encoding.UTF8.GetBytes("key");
            db.Delete((Slice)k);
            db.Get((Slice)k);
            Assert.True(false);
        }

        [Test]
        public void TestDatabaseOverwriteAll() {
            var enc = new UTF8Encoding();
            db.PushTablet("test-data/ngrams1/ngrams1-Nblock-compressed.tab");
            using (var data = new StreamReader("test-data/ngrams1/ngrams1.txt")) {
                var v = Encoding.UTF8.GetBytes("overwritten value");
                string line;
                while ((line = data.ReadLine()) != null) {
                    var kv = line.Split(new char[] {' '});
                    var k = enc.GetBytes(kv[0]);
                    db.Put((Slice)k, (Slice)v);
                }
                foreach (var p in db.Find()) {
                    Assert.True(p.Value.Equals((Slice)v));
                }
            }
        }

        [Test]
        public void TestDatabaseOverwriteFromN() {
            var n = 10;
            var enc = new UTF8Encoding();
            db.PushTablet("test-data/ngrams1/ngrams1-Nblock-compressed.tab");
            using (var data = new StreamReader("test-data/ngrams1/ngrams1.txt")) {
                var v = Encoding.UTF8.GetBytes("overwritten value");
                string line;
                var count = 0;
                while ((line = data.ReadLine()) != null) {
                    var kv = line.Split(new char[] {' '});
                    var k = enc.GetBytes(kv[0]);
                    if (count > n) {
                        db.Put((Slice)k, (Slice)v);
                    }
                    count += 1;
                }
            }
            using (var data = new StreamReader("test-data/ngrams1/ngrams1.txt")) {
                var ov = Encoding.UTF8.GetBytes("overwritten value");
                var count = 0;
                foreach (var p in db.Find()) {
                    var kv = data.ReadLine().Split(new char[] {' '});
                    var k = enc.GetBytes(kv[0]);
                    var v = enc.GetBytes(kv[1]);
                    Assert.True(p.Key.Equals((Slice)k));
                    if (count > n) {
                        Assert.True(p.Value.Equals((Slice)ov));
                    } else {
                        Assert.True(p.Value.Equals((Slice)v));
                    }
                    count += 1;
                }
                Assert.True(data.ReadLine() == null);
            }
        }

        [Test]
        public void TestDatabaseDeleteAll() {
            var enc = new UTF8Encoding();
            db.PushTablet("test-data/ngrams1/ngrams1-Nblock-compressed.tab");
            using (var data = new StreamReader("test-data/ngrams1/ngrams1.txt")) {
                string line;
                while ((line = data.ReadLine()) != null) {
                    var kv = line.Split(new char[] {' '});
                    var k = enc.GetBytes(kv[0]);
                    db.Delete((Slice)k);
                }
                var count = 0;
                foreach (var p in db.Find()) {
                    count += 1;
                }
                Assert.True(count == 0);
            }
        }

        [Test]
        public void TestDatabaseDeleteFromN() {
            var n = 10;
            var enc = new UTF8Encoding();
            db.PushTablet("test-data/ngrams1/ngrams1-Nblock-compressed.tab");
            using (var data = new StreamReader("test-data/ngrams1/ngrams1.txt")) {
                string line;
                var count = 0;
                while ((line = data.ReadLine()) != null) {
                    var kv = line.Split(new char[] {' '});
                    var k = enc.GetBytes(kv[0]);
                    if (count > n) {
                        db.Delete((Slice)k);
                    }
                    count += 1;
                }
            }
            using (var data = new StreamReader("test-data/ngrams1/ngrams1.txt")) {
                var count = 0;
                foreach (var p in db.Find()) {
                    var kv = data.ReadLine().Split(new char[] {' '});
                    var k = enc.GetBytes(kv[0]);
                    var v = enc.GetBytes(kv[1]);
                    Assert.True(p.Key.Equals((Slice)k));
                    Assert.True(p.Value.Equals((Slice)v));
                    count += 1;
                }
                Assert.True(count == n + 1);
                Assert.True(data.ReadLine() != null);
            }
        }

        [Test]
        public void TestPrefixQuery() {
            db.Put("aaaa", "aaaa");
            db.Put("key0", "val0");
            db.Put("key1", "val1");
            db.Put("key2", "val2");
            db.Put("key3", "val3");
            db.Put("key4", "val4");
            db.Put("zzzz", "zzzz");

            var iter = db.FindByPrefix(Utils.Slice("key")).GetEnumerator();
            for (int i = 0; i < 5; i++) {
                if (!iter.MoveNext()) {
                    Assert.Fail("unexpected end of prefix query");
                }
                Slice expected = Utils.Slice(String.Format("key{0}", i));
                Assert.True(iter.Current.Key.Equals(expected));
            }
            Assert.False(iter.MoveNext(), "extra items in prefix iteration: " + iter.Current.Key);
        }

        [Test]
        public void TestDatabaseRecursiveLock() {
            var nil = Utils.Slice("");

            db.Put(Utils.Slice("deleteme"), nil);
            db.Put(Utils.Slice("key1"), nil);
            db.Put(Utils.Slice("key2"), nil);

            // ensure we can modify the database while a read lock is held
            foreach (var kv in db.FindByPrefix("key")) {
                db.Delete("deleteme");
            }

            Assert.Throws(typeof(KeyNotFoundException),
                delegate { db.Get("deleteme"); });
        }
    }

    [TestFixture]
    public class DatabaseFileTests {
        private Database db;
        private string path;

        [SetUp]
        public void SetUp() {
            path = Path.Combine(Path.GetTempPath(), "test");

            Directory.CreateDirectory(path);
            db = Database.Open(path) as Database;
        }

        [TearDown]
        public void TearDown() {
            db.Close();
            Directory.Delete(path, true);
        }

        [Test]
        public void TestDatabasePushTabletStream() {
            var filename = Helpers.TestFile("ngrams1/ngrams1-Nblock-compressed.tab");
            using (var fs = new FileStream(filename, System.IO.FileMode.Open, System.IO.FileAccess.Read)) {
                db.PushTabletStream(fs, "streamed-tablet", null);
            }

            // Check that db contains all keys from the streamed tablet.
            var enc = new UTF8Encoding();
            using (var data = new StreamReader(Helpers.TestFile("ngrams1/ngrams1.txt"))) {
                foreach (var p in db.Find()) {
                    var kv = data.ReadLine().Split(new char[] {' '});
                    var k = enc.GetBytes(kv[0]);
                    var v = enc.GetBytes(kv[1]);

                    Assert.True(p.Key.Equals((Slice)k));
                    Assert.True(p.Value.Equals((Slice)v));
                }
                Assert.True(data.ReadLine() == null);
            }

            // Check that the emitted file matches the original.
            using (var fs1 = new FileStream(filename, System.IO.FileMode.Open, System.IO.FileAccess.Read))
            using (var fs2 = new FileStream(Path.Combine(path, "streamed-tablet"), System.IO.FileMode.Open, System.IO.FileAccess.Read)) {
                Assert.True(fs1.Length == fs2.Length);
                var count = 0;
                for (count = 0; count < fs1.Length; count++) {
                    if (fs1.ReadByte() != fs2.ReadByte()) {
                        break;
                    }
                }
                Assert.True(count == fs1.Length);
            }

            File.Delete(Path.Combine(path, "streamed-tablet"));
        }

        [Test]
        public void TestDatabaseReplay() {
            db.Put(Utils.Slice("key1"), Utils.Slice("val1"));
            db.Put(Utils.Slice("key2"), Utils.Slice("val2"));

            db.Put(Utils.Slice("key3"), Utils.Slice("oops"));
            db.Put(Utils.Slice("key3"), Utils.Slice("val3"));

            db.Put(Utils.Slice("key4"), Utils.Slice("deleteme"));
            db.Delete(Utils.Slice("key4"));

            db.Close();

            db = Database.Open(path) as Database;

            Assert.True(db.Get(Utils.Slice("key1")).Equals(Utils.Slice("val1")));
            Assert.True(db.Get(Utils.Slice("key2")).Equals(Utils.Slice("val2")));
            Assert.True(db.Get(Utils.Slice("key3")).Equals(Utils.Slice("val3")));

            Assert.Throws(typeof(KeyNotFoundException),
                delegate { db.Get(Utils.Slice("key4")); });
        }

        [Test]
        public void TestLogManyWriters()
        {
            // write a bunch of stuff simultaneously
            int nThreads = 5;
            int nItems = 100;

            ThreadStart ts = delegate {
                for (int i = 0; i < nItems; i++) {
                    db.Put(Utils.Slice("asdf"), Utils.Slice("fdsa"));
                }
            };

            var threads = new List<Thread>();
            for (int i = 0; i < nThreads; i++) {
                threads.Add(new Thread(ts));
            }

            foreach (var t in threads) {
                t.Start();
            }

            foreach (var t in threads) {
                t.Join();
            }

            db.Close();

            // replay the log
            var r = new TransactionLogReader(Path.Combine(path, "write.log"));
            foreach (var t in r.Transactions()) {
                // do nothing: rely on the internal consistency checks in r
            }
        }

        [Test]
        public void TestDatabaseMemCompact() {
            var value = (Slice)new byte[10000];

            for (int i = 0; i < 10000; i++) {
                var key = String.Format("{0:D8}", i);
                db.Put((Slice)Encoding.UTF8.GetBytes(key), value);
            }

            int count = 0;
            foreach (var kv in db.Find()) {
                count++;
            }

            Assert.True(count == 10000);
        }
    }
}
