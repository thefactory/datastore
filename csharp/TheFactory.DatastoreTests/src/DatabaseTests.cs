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
        public void TestOneTabletFindAll() {
            var enc = new UTF8Encoding();
            db.PushTablet("ngrams1/ngrams1-Nblock-compressed.tab");
            using (var data = new StreamReader("ngrams1/ngrams1.txt")) {
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
        public void TestMultiTabletFindAll() {
            var enc = new UTF8Encoding();
            db.PushTablet("ngrams2/ngrams.tab.0");
            db.PushTablet("ngrams2/ngrams.tab.1");
            using (var data = new StreamReader("ngrams2/ngrams2.txt")) {
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
        public void TestMultiTabletFindFromN() {
            var enc = new UTF8Encoding();
            db.PushTablet("ngrams2/ngrams.tab.0");
            db.PushTablet("ngrams2/ngrams.tab.1");
            using (var data = new StreamReader("ngrams2/ngrams2.txt")) {
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
        public void TestMultiTabletGetHit() {
            var enc = new UTF8Encoding();
            db.PushTablet("ngrams2/ngrams.tab.0");
            db.PushTablet("ngrams2/ngrams.tab.1");
            using (var data = new StreamReader("ngrams2/ngrams2.txt")) {
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
        public void TestMultiTabletGetMiss() {
            db.PushTablet("ngrams2/ngrams.tab.0");
            db.PushTablet("ngrams2/ngrams.tab.1");
            var keyString = "Key which does not exist";
            var k = Encoding.UTF8.GetBytes(keyString);
            db.Get(k);
            Assert.True(false);
        }
    }
}
