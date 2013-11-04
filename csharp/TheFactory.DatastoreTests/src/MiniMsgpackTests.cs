using System;
using NUnit.Framework;
using System.IO;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {

    [TestFixture]
    public class MiniMsgpackTests {
        public byte[] Read(MemoryStream buf, int len) {
            byte[] ret = new byte[len];
            buf.Position = 0;
            buf.Read(ret, 0, len);
            return ret;
        }

        struct EncodeTest {
            public int Num;
            public byte[] Bytes;
        }

        [Test]
        public void TestWriteRawLength () {
            // spot checks for raw lengths
            EncodeTest[] tests = new EncodeTest[] {
                new EncodeTest{ Num = 0, Bytes = new byte[] { 0xa0 } },
                new EncodeTest{ Num = 31, Bytes = new byte[] { 0xbf } },
                new EncodeTest{ Num = 32, Bytes = new byte[] { 0xda, 0x00, 0x20 } },
                new EncodeTest{ Num = 65535, Bytes = new byte[] { 0xda, 0xff, 0xff } },
                new EncodeTest{ Num = 65536, Bytes = new byte[] { 0xdb, 0x00, 0x01, 0x00, 0x00 } },
                new EncodeTest{ Num = 2147483647, Bytes = new byte[] { 0xdb, 0x7f, 0xff, 0xff, 0xff } },
                // current API uses signed ints and therefore doesn't support 4GB lengths
            };

            for (int i=0; i<tests.Length; i++) {
                var buf = new MemoryStream(10);
                EncodeTest test = tests[i];

                // make sure the expected number of bytes were written
                int len = MiniMsgpack.WriteRawLength(buf, test.Num);
                Assert.True(len == test.Bytes.Length);

                // check the exact expected bytes
                byte[] result = Read(buf, test.Bytes.Length);
                Assert.True(((Slice)test.Bytes).Equals((Slice)result), "test {0}: {1} != {2}",
                            i, (Slice)test.Bytes, (Slice)result);
            }
        }
    }
}

