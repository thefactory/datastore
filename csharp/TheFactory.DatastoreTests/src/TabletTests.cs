using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Snappy.Sharp;
using TheFactory.Datastore;
using MonoTouch.Foundation;

namespace TheFactory.DatastoreTests {
    public class Helpers {
        public static string TestFile(string relativePath) {
            return Path.Combine(Path.Combine(NSBundle.MainBundle.BundlePath, "test-data"), relativePath);
        }
    }

    [TestFixture]
    public class MemoryTabletTests {
        private MemoryTablet tablet;

        [SetUp]
        public void SetUp() {
            tablet = new MemoryTablet();
        }

        [TearDown]
        public void TearDown() {
            tablet.Close();
        }

        [Test]
        public void TestMemoryTabletSet() {
            var k = (Slice)Encoding.UTF8.GetBytes("key");
            var v = (Slice)Encoding.UTF8.GetBytes("value");
            tablet.Set(k, v);
            var count = 0;
            foreach (var p in tablet.Find(k)) {
                Assert.True(p.Value.Equals(v));
                count += 1;
                break;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestMemoryTabletReSet() {
            var k = (Slice)Encoding.UTF8.GetBytes("key");
            var v = (Slice)Encoding.UTF8.GetBytes("value");
            tablet.Set(k, (Slice)Encoding.UTF8.GetBytes("initial value"));
            tablet.Set(k, v);  // update value.
            var count = 0;
            foreach (var p in tablet.Find(k)) {
                Assert.True(p.Value.Equals(v));
                count += 1;
                break;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestMemoryTabletDelete() {
            var k = (Slice)Encoding.UTF8.GetBytes("key");
            var v = (Slice)Encoding.UTF8.GetBytes("value");
            tablet.Set(k, v);
            tablet.Delete(k);
            var count = 0;
            foreach (var p in tablet.Find(k)) {
                Assert.True(p.IsDeleted);
                count += 1;
                break;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestMemoryTabletEnumerateEmpty() {
            var count = 0;
            foreach (var p in tablet.Find()) {
                count += 1;
            }
            Assert.True(count == 0);
        }

        [Test]
        public void TestMemoryTabletEnumerateAll() {
            var pairs = new Slice[] {
                (Slice)Encoding.UTF8.GetBytes("key0"),
                (Slice)Encoding.UTF8.GetBytes("value0"),
                (Slice)Encoding.UTF8.GetBytes("key1"),
                (Slice)Encoding.UTF8.GetBytes("value1"),
                (Slice)Encoding.UTF8.GetBytes("key2"),
                (Slice)Encoding.UTF8.GetBytes("value2"),
                (Slice)Encoding.UTF8.GetBytes("key3"),
                (Slice)Encoding.UTF8.GetBytes("value3"),
                (Slice)Encoding.UTF8.GetBytes("key4"),
                (Slice)Encoding.UTF8.GetBytes("value4")
            };
            Assert.True(pairs.Length % 2 == 0);
            for (var i = 0; i < pairs.Length; i += 2) {
                tablet.Set(pairs[i], pairs[i + 1]);
            }

            var j = 0;
            foreach (var p in tablet.Find()) {
                Assert.True(p.Key.Equals(pairs[j]));
                Assert.True(p.Value.Equals(pairs[j + 1]));
                j += 2;
            }
            Assert.True(j == pairs.Length);
        }

        [Test]
        public void TestMemoryTabletEnumerateFromAfter() {
            var pairs = new Slice[] {
                (Slice)Encoding.UTF8.GetBytes("key0"),
                (Slice)Encoding.UTF8.GetBytes("value0"),
                (Slice)Encoding.UTF8.GetBytes("key1"),
                (Slice)Encoding.UTF8.GetBytes("value1"),
                (Slice)Encoding.UTF8.GetBytes("key2"),
                (Slice)Encoding.UTF8.GetBytes("value2"),
                (Slice)Encoding.UTF8.GetBytes("key3"),
                (Slice)Encoding.UTF8.GetBytes("value3"),
                (Slice)Encoding.UTF8.GetBytes("key4"),
                (Slice)Encoding.UTF8.GetBytes("value4")
            };
            Assert.True(pairs.Length % 2 == 0);
            for (var i = 0; i < pairs.Length; i += 2) {
                tablet.Set(pairs[i], pairs[i + 1]);
            }

            var count = 0;
            var term = (Slice)Encoding.UTF8.GetBytes("key5");  // After end.
            foreach (var p in tablet.Find(term)) {
                count += 1;
            }
            Assert.True(count == 0);
        }

        [Test]
        public void TestMemoryTabletEnumerateFromNFound() {
            var pairs = new Slice[] {
                (Slice)Encoding.UTF8.GetBytes("key0"),
                (Slice)Encoding.UTF8.GetBytes("value0"),
                (Slice)Encoding.UTF8.GetBytes("key1"),
                (Slice)Encoding.UTF8.GetBytes("value1"),
                (Slice)Encoding.UTF8.GetBytes("key2"),
                (Slice)Encoding.UTF8.GetBytes("value2"),
                (Slice)Encoding.UTF8.GetBytes("key3"),
                (Slice)Encoding.UTF8.GetBytes("value3"),
                (Slice)Encoding.UTF8.GetBytes("key4"),
                (Slice)Encoding.UTF8.GetBytes("value4")
            };
            Assert.True(pairs.Length % 2 == 0);
            for (var i = 0; i < pairs.Length; i += 2) {
                tablet.Set(pairs[i], pairs[i + 1]);
            }

            var j = 4;
            var term = pairs[j];
            foreach (var p in tablet.Find(term)) {
                Assert.True(p.Key.Equals(pairs[j]));
                Assert.True(p.Value.Equals(pairs[j + 1]));
                j += 2;
            }
            Assert.True(j == pairs.Length);
        }

        [Test]
        public void TestMemoryTabletEnumerateFromNNotFound() {
            var pairs = new Slice[] {
                (Slice)Encoding.UTF8.GetBytes("key0"),
                (Slice)Encoding.UTF8.GetBytes("value0"),
                (Slice)Encoding.UTF8.GetBytes("key1"),
                (Slice)Encoding.UTF8.GetBytes("value1"),
                (Slice)Encoding.UTF8.GetBytes("key2"),
                (Slice)Encoding.UTF8.GetBytes("value2"),
                (Slice)Encoding.UTF8.GetBytes("key3"),
                (Slice)Encoding.UTF8.GetBytes("value3"),
                (Slice)Encoding.UTF8.GetBytes("key4"),
                (Slice)Encoding.UTF8.GetBytes("value4")
            };
            Assert.True(pairs.Length % 2 == 0);
            for (var i = 0; i < pairs.Length; i += 2) {
                tablet.Set(pairs[i], pairs[i + 1]);
            }

            var j = 4;
            var term = (Slice)Encoding.UTF8.GetBytes("key11");  // After key1.
            foreach (var p in tablet.Find(term)) {
                Assert.True(p.Key.Equals(pairs[j]));
                Assert.True(p.Value.Equals(pairs[j + 1]));
                j += 2;
            }
            Assert.True(j == pairs.Length);
        }
    }

    [TestFixture]
    public class FileTabletTests {
        private byte[] SnappyCompressedBlock(byte[] data) {
            var c = new SnappyCompressor();

            int maxLen = c.MaxCompressedLength(data.Length);
            var buf = new byte[maxLen];
            var len = c.Compress(data, 0, data.Length, buf);

            var header = new byte[] {0, 1, (byte)len};
            var ret = new byte[header.Length + len];
            Buffer.BlockCopy(header, 0, ret, 0, header.Length);
            Buffer.BlockCopy(buf, 0, ret, header.Length, len);

            return ret;
        }

        [Test]
        public void TestTabletLoadBlockOneUncompressed() {
            // Simple block with header.
            var bytes = new byte[] {
                0,                          // H: checksum.
                0,                          // H: type (uncompressed).
                13,                         // H: length.
                0,                          // 0-byte key prefix.
                0xa3, 1, 2, 3,              // 3-byte key suffix.
                0xa3, 4, 5, 6,              // 3-byte value.
                0, 0, 0, 0                  // no restart indexes.
            };

            var block = new TabletBlock((Slice)bytes);
            var reader = new BlockReader(block.KvData);

            var count = 0;
            foreach (var p in reader.Find()) {
                Assert.True(p.Key.Equals((Slice)(new byte[] {1, 2, 3})));
                Assert.True(p.Value.Equals((Slice)(new byte[] {4, 5, 6})));
                count += 1;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestTabletLoadBlockOneSnappy() {
            var bytes = new byte[] {
                0,                          // H: checksum.
                1,                          // H: type (compressed).
                15,                         // H: length.

                // Snappy compressed: one kv pair, {1, 2, 3} => {4, 5, 6}
                // 0,                          // 0-byte key prefix.
                // 0xa3, 1, 2, 3,              // 3-byte key suffix.
                // 0xa3, 4, 5, 6,              // 3-byte value.
                // 0, 0, 0, 0                  // no restart indexes.
                0x0d, 0x30, 0x0, 0xa3, 0x1, 0x2, 0x3, 0xa3, 0x4, 0x5, 0x6, 0x0, 0x0, 0x0, 0x0
            };

            var block = new TabletBlock((Slice)bytes);
            var reader = new BlockReader(block.KvData);

            var count = 0;
            foreach (var p in reader.Find()) {
                Assert.True(p.Key.Equals((Slice)(new byte[] {1, 2, 3})));
                Assert.True(p.Value.Equals((Slice)(new byte[] {4, 5, 6})));
                count += 1;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestTabletFooterLoad() {
            var bytes = new byte[] {
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // MetaIndexOffset msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // MetaIndexLength msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // DataIndexOffset msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // DataIndexLength msgpack uint64.
                0x0b, 0x50, 0x1e, 0x7e         // Tablet magic (0x0b501e7e).
            };

            var reader = new TabletReader();
            var footer = reader.ParseFooter((Slice)bytes);

            Assert.True(footer.MetaIndexOffset == 0);
            Assert.True(footer.MetaIndexLength == 0);
            Assert.True(footer.DataIndexOffset == 0);
            Assert.True(footer.DataIndexLength == 0);
        }

        [Test]
        public void TestTabletFooterPaddedLoad() {
            // test various tighter encodings for the uints
            var bytes = new byte[] {
                0,                      // MetaIndexOffset (msgpack fixpos)
                0xcc, 0,                // MetaIndexLength (msgpack uint8)
                0xcd, 0, 0,             // DataIndexOffset (msgpack uint16)
                0xce, 0, 0, 0, 0,       // DataIndexLength (msgpack uint32)
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // padding
                0x0b, 0x50, 0x1e, 0x7e  // Tablet magic (0x0b501e7e).
            };

            var reader = new TabletReader();
            var footer = reader.ParseFooter((Slice)bytes);

            Assert.True(footer.MetaIndexOffset == 0);
            Assert.True(footer.MetaIndexLength == 0);
            Assert.True(footer.DataIndexOffset == 0);
            Assert.True(footer.DataIndexLength == 0);
        }

        [Test]
        [ExpectedException(typeof(TabletValidationException))]
        public void TestTabletFooterLoadBadMagic() {
            var bytes = new byte[] {
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // MetaIndexOffset msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // MetaIndexLength msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // DataIndexOffset msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // DataIndexLength msgpack uint64.
                0, 0, 0, 0                     // Bad tablet magic.
            };

            var reader = new TabletReader();
            reader.ParseFooter((Slice)bytes);

            Assert.True(false);  // ParseFooter() should throw.
        }

        [Test]
        public void TestTabletLoadIndexSimple() {
            var bytes = new byte[] {
                0, 0, 0, 0,       // magic (0).
                0, 10, 0xa1, 1,   // offset: 0, length: 10, data: 1.
                10, 10, 0xa1, 2,  // offset: 10, length: 10, data: 2.
            };

            var reader = new TabletReader();
            var index = reader.ParseIndex((Slice)bytes, 0);

            Assert.True(index.Count == 2);
            Assert.True(index[0].Offset == bytes[4]);
            Assert.True(index[0].Length == bytes[5]);
            Assert.True(index[0].Data[0] == bytes[7]);
            Assert.True(index[1].Offset == bytes[8]);
            Assert.True(index[1].Length == bytes[9]);
            Assert.True(index[1].Data[0] == bytes[11]);
        }

        [Test]
        [ExpectedException(typeof(TabletValidationException))]
        public void TestTabletLoadIndexBadMagic() {
            var bytes = new byte[] {
                0, 0, 0, 0,       // magic (0).
                0, 10, 0xa1, 1,   // offset: 0, length: 10, data: 1.
                10, 10, 0xa1, 2,  // offset: 10, length: 10, data: 2.
            };

            var reader = new TabletReader();
            reader.ParseIndex((Slice)bytes, 1); // look for (wrong) magic==1
            Assert.True(false);  // ParseIndex should throw.
        }

        [Test]
        public void TestTabletFindWithTermSimple() {
            // Hopefully the smallest possible tablet.
            var bytes = new byte[] {
                0,                              // H: checksum.
                0,                              // H: type (uncompressed).
                17,                             // H: length.
                0,                              // 0-byte key prefix.
                0xa3, 1, 2, 3,                  // 3-byte key suffix.
                0xa3, 4, 5, 6,                  // 3-byte value.
                0, 0, 0, 0,                     // restart at 0.
                0, 0, 0, 1,                     // one restart indexes.
                0x0e, 0xa7, 0xda, 0x7a,         // MetaIndex magic (0x0ea7da7a).
                0xda, 0x7a, 0xba, 0x5e,         // DataIndex magic (0xda7aba5e).
                0, 20, 0xa3, 1, 2, 3,           // offset: 0, length: 17, data: 1, 2, 3.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 20,  // MetaIndexOffset msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 4,   // MetaIndexLength msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 24,  // DataIndexOffset msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 10,  // DataIndexLength msgpack uint64.
                0x0b, 0x50, 0x1e, 0x7e          // Tablet magic (0x0b501e7e).
            };
            var stream = new MemoryStream(bytes);
            var tablet = new FileTablet(stream, new TabletReaderOptions());
            // Exactly match the only key in the tablet.
            var term = (Slice)(new byte[] {1, 2, 3});
            foreach (var p in tablet.Find(term)) {
                Assert.True(p.Key.Equals(term));
                Assert.True(p.Value.Equals((Slice)(new byte[] {4, 5, 6})));
                break;
            }
        }

        [Test]
        public void TestTabletFileUncompressed1BlockAll() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream(Helpers.TestFile("ngrams1/ngrams1-1block-uncompressed.tab"), FileMode.Open, FileAccess.Read)) {
                var tablet = new FileTablet(stream, new TabletReaderOptions());
                using (var data = new StreamReader(Helpers.TestFile("ngrams1/ngrams1.txt"))) {
                    foreach (var p in tablet.Find()) {
                        var kv = data.ReadLine().Split(new char[] {' '});
                        var k = enc.GetBytes(kv[0]);
                        var v = enc.GetBytes(kv[1]);
                        Assert.True(p.Key.Equals((Slice)k));
                        Assert.True(p.Value.Equals((Slice)v));
                    }
                    Assert.True(data.ReadLine() == null);
                }
            }
        }

        [Test]
        public void TestTabletFileUncompressed1BlockFrom1() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream(Helpers.TestFile("ngrams1/ngrams1-1block-uncompressed.tab"), FileMode.Open, FileAccess.Read)) {
                var tablet = new FileTablet(stream, new TabletReaderOptions());
                using (var data = new StreamReader(Helpers.TestFile("ngrams1/ngrams1.txt"))) {
                    // Read the first line to find a term.
                    var kv = data.ReadLine().Split(new char[] {' '});
                    var term = (Slice)enc.GetBytes(kv[0]);
                    foreach (var p in tablet.Find(term)) {
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
        }

        [Test]
        public void TestTabletFileUncompressed1BlockFromN() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream(Helpers.TestFile("ngrams1/ngrams1-1block-uncompressed.tab"), FileMode.Open, FileAccess.Read)) {
                var tablet = new FileTablet(stream, new TabletReaderOptions());
                using (var data = new StreamReader(Helpers.TestFile("ngrams1/ngrams1.txt"))) {
                    var count = 0;
                    string[] kv;
                    do {
                        count += 1;
                        kv = data.ReadLine().Split(new char[] {' '});
                    } while (count < 10);  // Skip some lines to find a term.
                    var term = (Slice)enc.GetBytes(kv[0]);
                    foreach (var p in tablet.Find(term)) {
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
        }

        [Test]
        public void TestTabletFileCompressed1BlockAll() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream(Helpers.TestFile("ngrams1/ngrams1-1block-compressed.tab"), FileMode.Open, FileAccess.Read)) {
                var tablet = new FileTablet(stream, new TabletReaderOptions());
                using (var data = new StreamReader(Helpers.TestFile("ngrams1/ngrams1.txt"))) {
                    foreach (var p in tablet.Find()) {
                        var kv = data.ReadLine().Split(new char[] {' '});
                        var k = enc.GetBytes(kv[0]);
                        var v = enc.GetBytes(kv[1]);
                        Assert.True(p.Key.Equals((Slice)k));
                        Assert.True(p.Value.Equals((Slice)v));
                    }
                    Assert.True(data.ReadLine() == null);
                }
            }
        }

        [Test]
        public void TestTabletFileCompressedNBlockAll() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream(Helpers.TestFile("ngrams1/ngrams1-Nblock-compressed.tab"), FileMode.Open, FileAccess.Read)) {
                var tablet = new FileTablet(stream, new TabletReaderOptions());
                using (var data = new StreamReader(Helpers.TestFile("ngrams1/ngrams1.txt"))) {
                    foreach (var p in tablet.Find()) {
                        var kv = data.ReadLine().Split(new char[] {' '});
                        var k = enc.GetBytes(kv[0]);
                        var v = enc.GetBytes(kv[1]);
                        Assert.True(p.Key.Equals((Slice)k));
                        Assert.True(p.Value.Equals((Slice)v));
                    }
                    Assert.True(data.ReadLine() == null);
                }
            }
        }

        [Test]
        public void TestTabletFileCompressedNBlockFrom1() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream(Helpers.TestFile("ngrams1/ngrams1-Nblock-compressed.tab"), FileMode.Open, FileAccess.Read)) {
                var tablet = new FileTablet(stream, new TabletReaderOptions());
                using (var data = new StreamReader(Helpers.TestFile("ngrams1/ngrams1.txt"))) {
                    // Read the first line to find a term.
                    var kv = data.ReadLine().Split(new char[] {' '});
                    var term = (Slice)enc.GetBytes(kv[0]);
                    foreach (var p in tablet.Find(term)) {
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
        }

        [Test]
        public void TestTabletFileCompressedNBlockFromN() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream(Helpers.TestFile("ngrams1/ngrams1-Nblock-compressed.tab"), FileMode.Open, FileAccess.Read)) {
                var tablet = new FileTablet(stream, new TabletReaderOptions());
                using (var data = new StreamReader(Helpers.TestFile("ngrams1/ngrams1.txt"))) {
                    var count = 0;
                    string[] kv;
                    do {
                        count += 1;
                        kv = data.ReadLine().Split(new char[] {' '});
                    } while (count < 10);  // Skip some lines to find a term.
                    var term = (Slice)enc.GetBytes(kv[0]);
                    foreach (var p in tablet.Find(term)) {
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
        }
    }

    [TestFixture]
    public class TabletWriterTests {
        private TabletWriter writer;

        [SetUp]
        public void SetUp() {
            writer = new TabletWriter();
        }

        [TearDown]
        public void TearDown() {
            writer = null;
        }

        [Test]
        public void TestTabletWriterUncompressed() {
            var writeStream = new MemoryStream();
            var binaryWriter = new BinaryWriter(writeStream);
            var filename = Helpers.TestFile("ngrams1/ngrams1-1block-uncompressed.tab");
            using (var stream = new FileStream(filename, FileMode.Open, FileAccess.Read)) {
                var tablet = new FileTablet(stream, new TabletReaderOptions());
                var opts = new TabletWriterOptions() {
                    BlockSize = 1024,
                    BlockCompression = false,
                    KeyRestartInterval = 10
                };
                writer.WriteTablet(binaryWriter, tablet.Find(), opts);
                var tablet2 = new FileTablet(writeStream, new TabletReaderOptions());
                var it1 = tablet.Find().GetEnumerator();
                var it2 = tablet2.Find().GetEnumerator();
                while (it1.MoveNext() && it2.MoveNext()) {
                    Assert.True(it1.Current.Key.Equals(it2.Current.Key));
                    Assert.True(it1.Current.Value.Equals(it2.Current.Value));
                }
                Assert.True(it1.MoveNext() == false);
                Assert.True(it2.MoveNext() == false);
            }
        }

        [Test]
        public void TestTabletWriterCompressed() {
            var writeStream = new MemoryStream();
            var binaryWriter = new BinaryWriter(writeStream);
            var filename = Helpers.TestFile("ngrams1/ngrams1-1block-uncompressed.tab");
            using (var stream = new FileStream(filename, FileMode.Open, FileAccess.Read)) {
                var tablet = new FileTablet(stream, new TabletReaderOptions());
                var opts = new TabletWriterOptions() {
                    BlockSize = 1024,
                    BlockCompression = true,
                    KeyRestartInterval = 10
                };
                writer.WriteTablet(binaryWriter, tablet.Find(), opts);
                var tablet2 = new FileTablet(writeStream, new TabletReaderOptions());
                var it1 = tablet.Find().GetEnumerator();
                var it2 = tablet2.Find().GetEnumerator();
                while (it1.MoveNext() && it2.MoveNext()) {
                    Assert.True(it1.Current.Key.Equals(it2.Current.Key));
                    Assert.True(it1.Current.Value.Equals(it2.Current.Value));
                }
                Assert.True(it1.MoveNext() == false);
                Assert.True(it2.MoveNext() == false);
            }
        }
    }
}
