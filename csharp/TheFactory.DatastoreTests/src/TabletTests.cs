using System;
using System.IO;
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
        public void TestTabletIteratorOneUncompressed() {
            // Simple block with header.
            var bytes = new byte[] {0,              // H: checksum.
                                    0,              // H: type (uncompressed).
                                    13,             // H: length.
                                    0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var stream = new MemoryStream(bytes);
            var tablet = new Tablet(stream);
            var count = 0;
            foreach (var p in tablet.Find()) {
                Assert.True(p.Key.CompareBytes(0, new byte[] {1, 2, 3}, 0, p.Key.Length));
                Assert.True(p.Value.CompareBytes(0, new byte[] {4, 5, 6}, 0, p.Value.Length));
                count += 1;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestTabletIteratorOneSnappy() {
            var blockBytes = new byte[] {0,              // 0-byte key prefix.
                                         0x3, 1, 2, 3,   // 3-byte key suffix.
                                         0x3, 4, 5, 6,   // 3-byte value.
                                         0, 0, 0, 0};    // no restart indexes.
            var bytes = SnappyCompressedBlock(blockBytes);
            var stream = new MemoryStream(bytes);
            var tablet = new Tablet(stream);
            var count = 0;
            foreach (var p in tablet.Find()) {
                Assert.True(p.Key.CompareBytes(0, new byte[] {1, 2, 3}, 0, p.Key.Length));
                Assert.True(p.Value.CompareBytes(0, new byte[] {4, 5, 6}, 0, p.Value.Length));
                count += 1;
            }
            Assert.True(count == 1);
        }
    }
}
