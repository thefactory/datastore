using System;
using NUnit.Framework;
using System.Collections.Generic;
using TheFactory.Datastore;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Collections;
using System.Diagnostics;
using System.Text;
using Splat;
using TheFactory.FileSystem;

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
        // bulkData is totally random data (hardly compressible).
        // textData is text (quite compressible).
        Slice bulkData;
        Slice textData;

        public ScaleTests () {
            // Initialize bulkData with 10MB of random but constantly seeded data.
            var rand = new Random(0);

            byte[] bulk = new byte[10 * Size.MB];
            rand.NextBytes(bulk);
            bulkData = (Slice)bulk;

            using (var r = new StreamReader(Helpers.TestFile("pg11.txt"))) {
                textData = (Slice)Encoding.UTF8.GetBytes(r.ReadToEnd());
            }
        }

        void TestRoundTrip(Options opts, int numWriters, IEnumerable<IKeyValuePair> goldenKVs) {
            Console.WriteLine("Round trip: numWriters = {0}", numWriters);
            Console.WriteLine("Options:\n{0}", opts.ToString());

            var dbPath = Path.Combine(Path.GetTempPath(), "db");
            if (Directory.Exists(dbPath)) {
                Directory.Delete(dbPath, true);
            }

            var taskOptions = new ParallelOptions();
            taskOptions.MaxDegreeOfParallelism = numWriters;

            long byteCount = 0;

            using (var db = Database.Open(dbPath, opts)) {
                // Write the golden KVs into the open database.
                var watch = Stopwatch.StartNew();

                Parallel.ForEach<IKeyValuePair>(goldenKVs, taskOptions, (kv) => {
                    if (kv.IsDeleted) {
                        db.Delete(kv.Key);
                        byteCount += kv.Key.Length;
                    } else {
                        db.Put(kv.Key, kv.Value);
                        byteCount += kv.Key.Length + kv.Value.Length;
                    }
                });

                LogRate("Wrote", byteCount, watch.ElapsedMilliseconds);

                // Check the values while the database is still open.
                watch = Stopwatch.StartNew();
                AssertEquals(db.Find().GetEnumerator(), goldenKVs.GetEnumerator());
                LogRate("Verified", byteCount, watch.ElapsedMilliseconds);

                // Close the database so we can check after opening it again.
                db.Close();
            }

            using (var db = Database.Open(dbPath, opts)) {
                AssertEquals(db.Find().GetEnumerator(), goldenKVs.GetEnumerator());
            }
        }

        void TestRoundTripWithDeletes(Options opts, int numWriters, IEnumerable<IKeyValuePair> goldenKVs) {
            Console.WriteLine("Round trip: numWriters = {0}", numWriters);
            Console.WriteLine("Options:\n{0}", opts.ToString());

            var dbPath = Path.Combine(Path.GetTempPath(), "db");
            if (Directory.Exists(dbPath)) {
                Directory.Delete(dbPath, true);
            }

            var taskOptions = new ParallelOptions();
            taskOptions.MaxDegreeOfParallelism = numWriters;

            long byteCount = 0;

            // Delete on average every 16th key, but since key[0:4] is a counter, run them through
            // a hash function to randomize the occurrence.
            Func<Slice, bool> shouldDelete = (Slice key) => {
                uint val = TheFactory.Datastore.Utils.ToUInt32(key);
                uint h = (val*0x1e35a7bd) & 0xf;
                return h == 0;
            };

            using (var db = Database.Open(dbPath, opts)) {
                // Write the golden KVs into the open database.
                var watch = Stopwatch.StartNew();

                Parallel.ForEach<IKeyValuePair>(goldenKVs, taskOptions, (kv) => {
                    db.Put(kv.Key, kv.Value);
                    byteCount += kv.Key.Length + kv.Value.Length;

                    if (shouldDelete(kv.Key)) {
                        db.Delete(kv.Key);
                        byteCount += kv.Key.Length;
                    }
                });

                LogRate("Wrote", byteCount, watch.ElapsedMilliseconds);

                // Check the values while the database is still open.
                watch = Stopwatch.StartNew();
                AssertEquals(db.Find().GetEnumerator(), goldenKVs.GetEnumerator(), shouldDelete);
                LogRate("Verified", byteCount, watch.ElapsedMilliseconds);

                // Close the database so we can check after opening it again.
                db.Close();
            }

            using (var db = Database.Open(dbPath, opts)) {
                AssertEquals(db.Find().GetEnumerator(), goldenKVs.GetEnumerator(), shouldDelete);
            }
        }

        void LogRate(string prefix, long byteCount, long millis) {
            double secs = (double)millis / 1000;
            double rate = byteCount / secs / Size.MB;

            Console.WriteLine("{0} {1} bytes in {2:F2}s: {3:F2} MB/sec", prefix, byteCount, secs, rate);
        }

        void AssertEquals(IEnumerator<IKeyValuePair> kvs, IEnumerator<IKeyValuePair> expected) {
            AssertEquals(kvs, expected, (key) => false);
        }

        void AssertEquals(IEnumerator<IKeyValuePair> kvs, IEnumerator<IKeyValuePair> expected, Func<Slice, bool> deleted) {
            int index = -1;
            while (true) {
                index++;

                var kvsNext = kvs.MoveNext();
                var expNext = expected.MoveNext();

                // Advance expected until it yields a non-deleted key.
                if (expNext && deleted(expected.Current.Key)) {
                    do {
                        expNext = expected.MoveNext();
                    } while (expNext && deleted(expected.Current.Key));
                }

                if (kvsNext && expNext) {
                    Assert.True(kvs.Current.Key.CompareTo(expected.Current.Key) == 0,
                        "[{0}] Unequal keys: {1} != {2}", index, kvs.Current.Key, expected.Current.Key);

                    if (deleted(expected.Current.Key)) {
                        Assert.True(kvs.Current.IsDeleted);
                    } else {
                        Assert.True(kvs.Current.Value.CompareTo(expected.Current.Value) == 0,
                            "[{0}] Unequal values: {1} != {2}", index, kvs.Current.Value, expected.Current.Value);
                    }

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
        public void OneWriterText100MB() {
            TestRoundTrip(new Options(), 1, new TestData(textData, 100 * Size.MB, 100, 10000));
        }

        [Test]
        public void OneWriterTextUncomp100MB() {
            var opts = new Options();
            opts.WriterOptions.BlockCompression = false;

            TestRoundTrip(opts, 1, new TestData(textData, 100 * Size.MB, 100, 10000));
        }

        [Test]
        public void TenWriters100MB() {
            TestRoundTrip(new Options(), 10, new TestData(bulkData, 100 * Size.MB, 100, 10000));
        }

        [Test]
        public void TenWriters10MBWithDeletes() {
            TestRoundTripWithDeletes(new Options(), 10, new TestData(bulkData, 10 * Size.MB, 100, 10000));
        }

        [Test]
        public void OneWriter10KBWithDeletes() {
            TestRoundTripWithDeletes(new Options(), 1, new TestData(bulkData, 40, 8, 8));
        }

        [Test]
        public void TenWriters1GB() {
            var opts = new Options();
            opts.MaxMemoryTabletSize = 100 * Size.MB;

            TestRoundTrip(opts, 10, new TestData(bulkData, 1 * Size.GB, 100, 10000));
        }

        [Test]
        public void TenWriters10GB() {
            var opts = new Options();
            opts.MaxMemoryTabletSize = 100 * Size.MB;

            TestRoundTrip(opts, 10, new TestData(bulkData, 10 * (long)Size.GB, 100, 10000));
        }

        [Test]
        public void OneWriterWithGets10MB() {
            var opts = new Options();

            var dbPath = Path.Combine(Path.GetTempPath(), "db");
            if (Directory.Exists(dbPath)) {
                Directory.Delete(dbPath, true);
            }

            Slice key1 = (Slice)Encoding.ASCII.GetBytes("key1");
            Slice val1 = (Slice)Encoding.ASCII.GetBytes("value1");
            Slice key2 = null;
            Slice val2 = null;

            using (var db = Database.Open(dbPath, opts)) {
                db.Put(key1, val1);

                foreach (var kv in new TestData(bulkData, 10 * Size.MB, 100, 10000)) {
                    db.Put(kv.Key, kv.Value);

                    Assert.IsTrue(db.Get(key1).CompareTo(val1) == 0);
                    if (key2 != null) {
                        Assert.IsTrue(db.Get(key2).CompareTo(val2) == 0);
                    }

                    key2 = kv.Key;
                    val2 = kv.Value;
                }
            }
        }
    }

    public class TestData: IEnumerable, IEnumerable<IKeyValuePair> {
        Slice bulkData;
        long totalBytes;
        int avgKeyLength;
        int avgValueLength;

        public TestData(Slice bulkData, long totalBytes, int avgKeyLength, int avgValueLength) {
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
            uint numKeys = 0;
            long count = 0;
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

