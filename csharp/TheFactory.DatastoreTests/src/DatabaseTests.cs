using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {
    [TestFixture]
    public class DatabaseTests {
        private Database db;

        [SetUp]
        public void SetUp() {
            db = new Database();
        }

        [TearDown]
        public void TearDown() {
            db.Close();
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
                    Assert.True(p.Key.CompareBytes(0, k, 0, p.Key.Length));
                    Assert.True(p.Value.CompareBytes(0, v, 0, p.Value.Length));
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
                    Assert.True(p.Key.CompareBytes(0, k, 0, p.Key.Length));
                    Assert.True(p.Value.CompareBytes(0, v, 0, p.Value.Length));
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
                var term = enc.GetBytes(kv[0]);
                foreach (var p in db.Find(term)) {
                    var k = enc.GetBytes(kv[0]);
                    var v = enc.GetBytes(kv[1]);
                    Assert.True(p.Key.CompareBytes(0, k, 0, p.Key.Length));
                    Assert.True(p.Value.CompareBytes(0, v, 0, p.Value.Length));
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
                var result = db.Get(k);
                Assert.True(result.CompareBytes(0, v, 0, v.Length));
            }
        }

        [Test]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestDatabaseMultiFileTabletGetMiss() {
            db.PushTablet("test-data/ngrams2/ngrams.tab.0");
            db.PushTablet("test-data/ngrams2/ngrams.tab.1");
            var keyString = "Key which does not exist";
            var k = Encoding.UTF8.GetBytes(keyString);
            db.Get(k);
            Assert.True(false);
        }

        [Test]
        public void TestDatabaseMemoryOnlyWrite() {
            var k = Encoding.UTF8.GetBytes("key");
            var v = Encoding.UTF8.GetBytes("value");
            db.Put(k, v);
            var val = db.Get(k);
            Assert.True(val.CompareBytes(0, v, 0, v.Length));
        }

        [Test]
        public void TestDatabaseMemoryOnlyReWrite() {
            var k = Encoding.UTF8.GetBytes("key");
            var v = Encoding.UTF8.GetBytes("value");
            db.Put(k, Encoding.UTF8.GetBytes("initial value"));
            db.Put(k, v);
            var val = db.Get(k);
            Assert.True(val.CompareBytes(0, v, 0, v.Length));
        }

        [Test]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void TestDatabaseMemoryOnlyDelete() {
            var k = Encoding.UTF8.GetBytes("key");
            db.Delete(k);
            db.Get(k);
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
                    db.Put(k, v);
                }
                foreach (var p in db.Find()) {
                    Assert.True(p.Value.CompareBytes(0, v, 0, v.Length));
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
                        db.Put(k, v);
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
                    Assert.True(p.Key.CompareBytes(0, k, 0, p.Key.Length));
                    if (count > n) {
                        Assert.True(p.Value.CompareBytes(0, ov, 0, ov.Length));
                    } else {
                        Assert.True(p.Value.CompareBytes(0, v, 0, v.Length));
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
                    db.Delete(k);
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
                        db.Delete(k);
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
                    Assert.True(p.Key.CompareBytes(0, k, 0, p.Key.Length));
                    Assert.True(p.Value.CompareBytes(0, v, 0, v.Length));
                    count += 1;
                }
                Assert.True(count == n + 1);
                Assert.True(data.ReadLine() != null);
            }
        }
    }
}
