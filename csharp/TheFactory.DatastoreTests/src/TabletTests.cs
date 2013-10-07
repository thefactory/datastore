using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Snappy.Sharp;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {
    [TestFixture]
    public class TabletTests {
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
            var stream = new MemoryStream(bytes);
            var tablet = new Tablet(stream);
            var block = tablet.LoadBlock(0);
            var count = 0;
            foreach (var p in block.Find()) {
                Assert.True(p.Key.CompareBytes(0, new byte[] {1, 2, 3}, 0, p.Key.Length));
                Assert.True(p.Value.CompareBytes(0, new byte[] {4, 5, 6}, 0, p.Value.Length));
                count += 1;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestTabletLoadBlockOneSnappy() {
            var blockBytes = new byte[] {
                0,                          // 0-byte key prefix.
                0xa3, 1, 2, 3,              // 3-byte key suffix.
                0xa3, 4, 5, 6,              // 3-byte value.
                0, 0, 0, 0                  // no restart indexes.
            };
            var bytes = SnappyCompressedBlock(blockBytes);
            var stream = new MemoryStream(bytes);
            var tablet = new Tablet(stream);
            var block = tablet.LoadBlock(0);
            var count = 0;
            foreach (var p in block.Find()) {
                Assert.True(p.Key.CompareBytes(0, new byte[] {1, 2, 3}, 0, p.Key.Length));
                Assert.True(p.Value.CompareBytes(0, new byte[] {4, 5, 6}, 0, p.Value.Length));
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
            var stream = new MemoryStream(bytes);
            var tablet = new Tablet(stream);
            var footer = tablet.LoadFooter();
            Assert.True(footer.MetaIndexOffset == 0);
            Assert.True(footer.MetaIndexLength == 0);
            Assert.True(footer.DataIndexOffset == 0);
            Assert.True(footer.DataIndexLength == 0);
        }

        [Test]
        public void TestTabletFooterLoadBadMagic() {
            var bytes = new byte[] {
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // MetaIndexOffset msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // MetaIndexLength msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // DataIndexOffset msgpack uint64.
                0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // DataIndexLength msgpack uint64.
                0, 0, 0, 0                     // Bad tablet magic.
            };
            var stream = new MemoryStream(bytes);
            var tablet = new Tablet(stream);
            try {
                tablet.LoadFooter();
                Assert.True(false);  // LoadFooter() should throw.
            } catch (TabletValidationException e) {
                Assert.True(true);  // Expecting this due to bad magic.
            }
        }

        [Test]
        public void TestTabletLoadIndexSimple() {
            var bytes = new byte[] {
                0, 0, 0, 0,       // magic (0).
                0, 10, 0xa1, 1,   // offset: 0, length: 10, data: 1.
                10, 10, 0xa1, 2,  // offset: 10, length: 10, data: 2.
            };
            var stream = new MemoryStream(bytes);
            var tablet = new Tablet(stream);
            var index = tablet.LoadIndex(0, 10, 0);
            Assert.True(index.Count == 2);
            Assert.True(index[0].Offset == bytes[4]);
            Assert.True(index[0].Length == bytes[5]);
            Assert.True(index[0].Data[0] == bytes[7]);
            Assert.True(index[1].Offset == bytes[8]);
            Assert.True(index[1].Length == bytes[9]);
            Assert.True(index[1].Data[0] == bytes[11]);
        }

        [Test]
        public void TestTabletLoadIndexBadMagic() {
            var bytes = new byte[] {
                0, 0, 0, 0,       // magic (0).
                0, 10, 0xa1, 1,   // offset: 0, length: 10, data: 1.
                10, 10, 0xa1, 2,  // offset: 10, length: 10, data: 2.
            };
            var stream = new MemoryStream(bytes);
            var tablet = new Tablet(stream);
            try {
                tablet.LoadIndex(0, 10, 1);  // send magic 1.
                Assert.True(false);  // LoadIndex should throw.
            } catch (TabletValidationException e) {
                Assert.True(true);  // Expecting this due to bad magic.
            }
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
            var tablet = new Tablet(stream);
            // Exactly match the only key in the tablet.
            var term = new byte[] {1, 2, 3};
            foreach (var p in tablet.Find(term)) {
                Assert.True(p.Key.CompareBytes(0, term, 0, p.Key.Length));
                Assert.True(p.Value.CompareBytes(0, new byte[] {4, 5, 6}, 0, p.Value.Length));
                break;
            }
        }

        [Test]
        public void TestTabletFileUncompressed1BlockAll() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream("ngrams1/ngrams1-1block-uncompressed.tab", FileMode.Open)) {
                var tablet = new Tablet(stream);
                using (var data = new StreamReader("ngrams1/ngrams1.txt")) {
                    foreach (var p in tablet.Find()) {
                        var kv = data.ReadLine().Split(new char[] {' '});
                        var k = enc.GetBytes(kv[0]);
                        var v = enc.GetBytes(kv[1]);
                        Assert.True(p.Key.CompareBytes(0, k, 0, p.Key.Length));
                        Assert.True(p.Value.CompareBytes(0, v, 0, p.Value.Length));
                    }
                    Assert.True(data.ReadLine() == null);
                }
            }
        }

        [Test]
        public void TestTabletFileUncompressed1BlockFrom1() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream("ngrams1/ngrams1-1block-uncompressed.tab", FileMode.Open)) {
                var tablet = new Tablet(stream);
                using (var data = new StreamReader("ngrams1/ngrams1.txt")) {
                    // Read the first line to find a term.
                    var kv = data.ReadLine().Split(new char[] {' '});
                    var term = enc.GetBytes(kv[0]);
                    foreach (var p in tablet.Find(term)) {
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
        }

        [Test]
        public void TestTabletFileUncompressed1BlockFromN() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream("ngrams1/ngrams1-1block-uncompressed.tab", FileMode.Open)) {
                var tablet = new Tablet(stream);
                using (var data = new StreamReader("ngrams1/ngrams1.txt")) {
                    var count = 0;
                    string[] kv;
                    do {
                        count += 1;
                        kv = data.ReadLine().Split(new char[] {' '});
                    } while (count < 10);  // Skip some lines to find a term.
                    var term = enc.GetBytes(kv[0]);
                    foreach (var p in tablet.Find(term)) {
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
        }

        [Test]
        public void TestTabletFileCompressed1BlockAll() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream("ngrams1/ngrams1-1block-compressed.tab", FileMode.Open)) {
                var tablet = new Tablet(stream);
                using (var data = new StreamReader("ngrams1/ngrams1.txt")) {
                    foreach (var p in tablet.Find()) {
                        var kv = data.ReadLine().Split(new char[] {' '});
                        var k = enc.GetBytes(kv[0]);
                        var v = enc.GetBytes(kv[1]);
                        Assert.True(p.Key.CompareBytes(0, k, 0, p.Key.Length));
                        Assert.True(p.Value.CompareBytes(0, v, 0, p.Value.Length));
                    }
                    Assert.True(data.ReadLine() == null);
                }
            }
        }

        [Test]
        public void TestTabletFileCompressedNBlockAll() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream("ngrams1/ngrams1-Nblock-compressed.tab", FileMode.Open)) {
                var tablet = new Tablet(stream);
                using (var data = new StreamReader("ngrams1/ngrams1.txt")) {
                    foreach (var p in tablet.Find()) {
                        var kv = data.ReadLine().Split(new char[] {' '});
                        var k = enc.GetBytes(kv[0]);
                        var v = enc.GetBytes(kv[1]);
                        Assert.True(p.Key.CompareBytes(0, k, 0, p.Key.Length));
                        Assert.True(p.Value.CompareBytes(0, v, 0, p.Value.Length));
                    }
                    Assert.True(data.ReadLine() == null);
                }
            }
        }

        [Test]
        public void TestTabletFileCompressedNBlockFrom1() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream("ngrams1/ngrams1-Nblock-compressed.tab", FileMode.Open)) {
                var tablet = new Tablet(stream);
                using (var data = new StreamReader("ngrams1/ngrams1.txt")) {
                    // Read the first line to find a term.
                    var kv = data.ReadLine().Split(new char[] {' '});
                    var term = enc.GetBytes(kv[0]);
                    foreach (var p in tablet.Find(term)) {
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
        }

        [Test]
        public void TestTabletFileCompressedNBlockFromN() {
            var enc = new UTF8Encoding();
            using (var stream = new FileStream("ngrams1/ngrams1-Nblock-compressed.tab", FileMode.Open)) {
                var tablet = new Tablet(stream);
                using (var data = new StreamReader("ngrams1/ngrams1.txt")) {
                    var count = 0;
                    string[] kv;
                    do {
                        count += 1;
                        kv = data.ReadLine().Split(new char[] {' '});
                    } while (count < 10);  // Skip some lines to find a term.
                    var term = enc.GetBytes(kv[0]);
                    foreach (var p in tablet.Find(term)) {
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
        }
    }
}
