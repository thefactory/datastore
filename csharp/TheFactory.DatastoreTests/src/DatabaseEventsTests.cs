using System;
using NUnit.Framework;
using System.Collections.Generic;
using TheFactory.Datastore;
using System.IO;

namespace TheFactory.DatastoreTests {
    [TestFixture]
    public class DatabaseEventsTests {
        Slice bulkData;

        public DatabaseEventsTests() {
            // Initialize bulkData with 10MB of random but constantly seeded data.
            var rand = new Random(0);

            byte[] bulk = new byte[10 * Size.MB];
            rand.NextBytes(bulk);
            bulkData = (Slice)bulk;
        }

        [Test]
        public void TestBulk() {
            var kvs = new TestData(bulkData, 1 * Size.MB, 100, 10000);
            EventRoundTrip(kvs);
        }


        [Test]
        public void TestWithDeletes() {
            List<IKeyValuePair> kvs = new List<IKeyValuePair>();
            var rand = new Random(0);

            // Make a list of test data with 10% of the keys deleted.
            foreach (var td in new TestData(bulkData, 1 * Size.MB, 100, 10000)) {
                // Jump through a temporary variable hoop because td can't be assigned.
                IKeyValuePair kv = td;
                if (rand.Next(10) == 0) {
                    // Replace kv with a deleted key.
                    var tmp = new Pair();
                    tmp.Key = kv.Key;
                    tmp.IsDeleted = true;
                    kv = tmp;
                }

                kvs.Add(kv);
            }

            EventRoundTrip(kvs);
        }

        void EventRoundTrip(IEnumerable<IKeyValuePair> testdata) {
            var dbPath = Path.Combine(Path.GetTempPath(), "db");
            if (Directory.Exists(dbPath)) {
                Directory.Delete(dbPath, true);
            }

            // Add an event handler to the database, insert all the testdata,
            // and accumulate the kv events in a list. Verify the database
            // contains the same data as the list.

            List<IKeyValuePair> seen = new List<IKeyValuePair>();
            using (var db = Database.Open(dbPath, new Options())) {
                db.KeyValueChangedEvent += (sender, e) => {
                    seen.Add(e.Pair);
                };

                // Insert the test data.
                foreach (var kv in testdata) {
                    if (kv.IsDeleted) {
                        db.Delete(kv.Key);
                    } else {
                        db.Put(kv.Key, kv.Value);
                    }
                }

                AssertEquals(seen.GetEnumerator(), testdata.GetEnumerator());
            }
        }

        void AssertEquals(IEnumerator<IKeyValuePair> kvs, IEnumerator<IKeyValuePair> expected) {
            while (true) {
                var a = kvs.MoveNext();
                var b = expected.MoveNext();

                if (a && b) {
                    Assert.True(a.CompareTo(b) == 0);
                }

                if (!a && b) {
                    Assert.Fail("Short kv enumerator");
                }

                if (a && !b) {
                    Assert.Fail("Long kv enumerator");
                }

                if (!a && !b) {
                    // success
                    break;
                }
            }
        }
    }
}

