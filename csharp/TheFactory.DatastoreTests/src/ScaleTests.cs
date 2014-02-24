using System;
using NUnit.Framework;
using System.Collections.Generic;
using TheFactory.Datastore;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Collections;

namespace TheFactory.DatastoreTests {

    class Size {
        public const int KB = 1024;
        public const int MB = 1024 * KB;
        public const int GB = 1024 * MB;
    }

    // These scale tests are marked Explicit so they won't be run in
    // the default test runner.
    [TestFixture, Explicit]
    public class ScaleTests {
        Slice bulkData;

        public ScaleTests () {
            // Initialize bulkData with 10MB of random but constantly seeded data.
            var rand = new Random(0);

            byte[] bulk = new byte[10 * Size.MB];
            rand.NextBytes(bulk);
            bulkData = (Slice)bulk;
        }

        void TestRoundTrip(Options opts, int numWriters, IEnumerable<IKeyValuePair> goldenKVs) {
            var dbPath = Path.Combine(Path.GetTempPath(), "db");
            Directory.Delete(dbPath, true);

            var taskOptions = new ParallelOptions();
            taskOptions.MaxDegreeOfParallelism = numWriters;

            using (var db = Database.Open(dbPath, opts)) {
                // Write the golden KVs into the open database.
                Parallel.ForEach<IKeyValuePair>(goldenKVs, taskOptions, (kv) => {
                    if (kv.IsDeleted) {
                        db.Delete(kv.Key);
                    } else {
                        db.Put(kv.Key, kv.Value);
                    }
                });

                // Check the values while the database is still open.
                AssertEquals(db.Find().GetEnumerator(), goldenKVs.GetEnumerator());

                // Close the database so we can check after opening it again.
                db.Close();
            }

            using (var db = Database.Open(dbPath, opts)) {
                AssertEquals(db.Find().GetEnumerator(), goldenKVs.GetEnumerator());
            }
        }

        void AssertEquals(IEnumerator<IKeyValuePair> kvs, IEnumerator<IKeyValuePair> expected) {
            int index = -1;
            while (true) {
                index++;

                var kvsNext = kvs.MoveNext();
                var expNext = expected.MoveNext();

                if (kvsNext && expNext) {
                    Assert.True(kvs.Current.Key.CompareTo(expected.Current.Key) == 0,
                        "[{0}] Unequal keys: {1} != {2}", index, kvs.Current.Key, expected.Current.Key);
                    Assert.True(kvs.Current.Value.CompareTo(expected.Current.Value) == 0,
                        "[{0}] Unequal values: {1} != {2}", index, kvs.Current.Value, expected.Current.Value);

                    continue;
                }

                if (!kvsNext && expNext) {
                    Assert.Fail("Short kvs enumerator: expected has more items");
                    break;
                }

                if (kvsNext && !expNext) {
                    Assert.Fail("Long kvs enumerator: expected is now empty");
                    break;
                }

                if (!kvsNext && !expNext) {
                    // Both sequences are empty: success
                    break;
                }
            }
        }

        [Test]
        public void OneWriter10KB() {
            // Test a totally in-memory round trip with small keys and values.
            TestRoundTrip(new Options(), 1, new TestData(bulkData, 10 * Size.KB, 8, 8));
        }

        [Test]
        public void OneWriter5MB() {
            // Test a round trip with a single immutable tablet write.
            TestRoundTrip(new Options(), 1, new TestData(bulkData, 5 * Size.MB, 100, 10000));
        }

        [Test]
        public void OneWriter100MB() {
            TestRoundTrip(new Options(), 1, new TestData(bulkData, 100 * Size.MB, 100, 10000));
        }

        [Test]
        public void TenWriters100MB() {
            TestRoundTrip(new Options(), 10, new TestData(bulkData, 100 * Size.MB, 100, 10000));
        }
    }

    public class TestData: IEnumerable, IEnumerable<IKeyValuePair> {
        Slice bulkData;
        int totalBytes;
        int avgKeyLength;
        int avgValueLength;

        public TestData(Slice bulkData, int totalBytes, int avgKeyLength, int avgValueLength) {
            this.bulkData = bulkData;
            this.totalBytes = totalBytes;
            this.avgKeyLength = avgKeyLength;
            this.avgValueLength = avgValueLength;
        }

        IEnumerator IEnumerable.GetEnumerator ()
        {
            return GetEnumerator();
        }

        IEnumerator<IKeyValuePair> IEnumerable<IKeyValuePair>.GetEnumerator() {
            return GetEnumerator();
        }

        IEnumerator<IKeyValuePair> GetEnumerator() {
            var rand = new Random(0);

            // Generate count bytes worth of KV pairs.
            //
            // Produce keys of length 4..2*avgKeyLength+5. To reuse this function for both
            // data production and checking, the keys must be monotonically increasing:
            // overwrite the first 4 bytes with a counter to accomplish that.
            //
            // Produce values of 0..2*avgValueLength so we do get occasional empty values.
            //
            // Since rand is seeded with a constant, it will always produce the same kv pairs
            // for the same avgKeyLength and avgValueLength arguments. This allows us to
            // work with data sets (both inserting and checking afterward) that don't fit in
            // memory.
            //
            // Keys must be monotonically increasing; otherwise this can't be used to verify
            // data after it's sorted by the database.
            UInt32 numKeys = 0;
            int count = 0;
            int pos = 0;
            while (count < totalBytes) {
                var pair = new Pair();

                var keyLength = rand.Next(4, 2 * avgKeyLength + 5);
                var valLength = rand.Next(0, 2 * avgValueLength + 1);

                if (pos + keyLength > bulkData.Offset + bulkData.Length) {
                    pos = 0;
                }

                var key = bulkData.Subslice(pos, keyLength).Detach();
                key[0] = (byte)((numKeys >> 24) & 0xFF);
                key[1] = (byte)((numKeys >> 16) & 0xFF);
                key[2] = (byte)((numKeys >> 8) & 0xFF);
                key[3] = (byte)(numKeys++ & 0xFF);

                pair.Key = key;
                pos += keyLength;

                if (pos + valLength > bulkData.Offset + bulkData.Length) {
                    pos = 0;
                }
                pair.Value = bulkData.Subslice(pos, valLength);
                pos += valLength;

                count += (keyLength + valLength);

                yield return pair;
            }

            yield break;
        }
    }
}

