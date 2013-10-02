using System;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests
{
    [TestFixture]
    public class BlockTests
    {
        private bool CompareByteArray(byte[] b1, int b1Offset, byte[] b2, int b2Offset, int count) {
            for (var i = 0; i < count; i++) {
                if (b1[b1Offset + i] != b2[b2Offset + i]) {
                    return false;
                }
            }

            return true;
        }

        [Test]
        public void TestBlockIterateOne() {
            // Simple block segment without key prefix.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var count = 0;
            foreach (var p in block.Find()) {
                Assert.True(CompareByteArray(p.Key, 0, new byte[] {1, 2, 3}, 0, p.Key.Length));
                Assert.True(CompareByteArray(p.Value, 0, new byte[] {4, 5, 6}, 0, p.Value.Length));
                count += 1;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestBlockIterateManyNoPrefix() {
            // Simple block segment without key prefixes.
            // Note: we don't enforce unique keys.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var count = 0;
            foreach (var p in block.Find()) {
                Assert.True(CompareByteArray(p.Key, 0, new byte[] {1, 2, 3}, 0, p.Key.Length));
                Assert.True(CompareByteArray(p.Value, 0, new byte[] {4, 5, 6}, 0, p.Value.Length));
                count += 1;
            }
            Assert.True(count == 2);
        }

        [Test]
        public void TestBlockIterateManyWithPrefix() {
            // Simple block segment without key prefixes.
            // Note: we don't enforce unique keys.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    1,              // 1-byte key prefix.
                                    0x2, 2, 3,      // 2-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    2,              // 2-byte key prefix.
                                    0x1, 3,         // 1-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    3,              // 3-byte key prefix.
                                    0x0,            // 0-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var count = 0;
            foreach (var p in block.Find()) {
                Assert.True(CompareByteArray(p.Key, 0, new byte[] {1, 2, 3}, 0, p.Key.Length));
                Assert.True(CompareByteArray(p.Value, 0, new byte[] {4, 5, 6}, 0, p.Value.Length));
                count += 1;
            }
            Assert.True(count == 4);
        }

        [Test]
        public void TestBlockWithRestarts() {
            // Simple block segment without key prefixes.
            // Note: we don't enforce unique keys.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 9,     // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var count = 0;
            foreach (var p in block.Find()) {
                Assert.True(CompareByteArray(p.Key, 0, new byte[] {1, 2, 3}, 0, p.Key.Length));
                Assert.True(CompareByteArray(p.Value, 0, new byte[] {4, 5, 6}, 0, p.Value.Length));
                count += 1;
            }
            Assert.True(count == 2);
        }

        [Test]
        public void TestBlockMidStream() {
            // Simple block starts and ends mid-[tablet]-stream.
            // This is really three data blocks in a row.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0,     // no restart indexes.
                                    0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0,     // no restart indexes.
                                    0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new Block(bytes, 13, 13);  // read the middle block.
            var count = 0;
            foreach (var p in block.Find()) {
                Assert.True(CompareByteArray(p.Key, 0, new byte[] {1, 2, 3}, 0, p.Key.Length));
                Assert.True(CompareByteArray(p.Value, 0, new byte[] {4, 5, 6}, 0, p.Value.Length));
                count += 1;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestBlockFindOnRestart() {
            // Simple block segment without key prefixes.
            // Note: we don't enforce unique keys.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0x3, 2, 3, 4,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0x3, 3, 4, 5,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 9,     // second restart.
                                    0, 0, 0, 18,    // third restart.
                                    0, 0, 0, 3};    // 3 restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var term = new byte[] {2, 3, 4};
            foreach (var p in block.Find(term)) {
                Assert.True(CompareByteArray(p.Key, 0, term, 0, p.Key.Length));
                break;
            }
        }

        [Test]
        public void TestBlockFindOffRestart() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 1-byte key prefix.
                                    0x3, 1, 2, 4,   // 2-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 2-byte key prefix.
                                    0x3, 1, 2, 5,   // 1-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 3-byte key prefix.
                                    0x3, 1, 2, 6,   // 0-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 18,    // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var term = new byte[] {1, 2, 4};
            foreach (var p in block.Find(term)) {
                Assert.True(CompareByteArray(p.Key, 0, term, 0, p.Key.Length));
                break;
            }
        }

        [Test]
        public void TestBlockFindNoRestarts() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    1,              // 1-byte key prefix.
                                    0x2, 2, 4,      // 2-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    2,              // 2-byte key prefix.
                                    0x1, 5,         // 1-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    2,              // 3-byte key prefix.
                                    0x1, 6,         // 0-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var term = new byte[] {1, 2, 5};
            foreach (var p in block.Find(term)) {
                Assert.True(CompareByteArray(p.Key, 0, term, 0, p.Key.Length));
                break;
            }
        }

        [Test]
        public void TestBlockFindUnmatchedBefore() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 1-byte key prefix.
                                    0x3, 1, 2, 4,   // 2-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 2-byte key prefix.
                                    0x3, 1, 2, 5,   // 1-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 3-byte key prefix.
                                    0x3, 1, 2, 6,   // 0-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 18,    // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var expected = new byte[] {1, 2, 3};
            foreach (var p in block.Find(new byte[] {0, 1, 2})) {
                Assert.True(CompareByteArray(p.Key, 0, expected, 0, p.Key.Length));
                break;
            }
        }

        [Test]
        public void TestBlockFindUnmatchedAfter() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 1-byte key prefix.
                                    0x3, 1, 2, 4,   // 2-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 2-byte key prefix.
                                    0x3, 1, 2, 5,   // 1-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 3-byte key prefix.
                                    0x3, 1, 2, 6,   // 0-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 18,    // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var count = 0;
            foreach (var p in block.Find(new byte[] {2, 3, 4})) {
                count += 1;
                break;
            }
            // Iterator returned by Find is at its end; no iteration.
            Assert.True(count == 0);
        }

        [Test]
        public void TestBlockFindUnmatchedMiddle() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0x3, 1, 2, 3,   // 3-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 1-byte key prefix.
                                    0x3, 1, 2, 4,   // 2-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 2-byte key prefix.
                                    0x3, 1, 2, 5,   // 1-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0,              // 3-byte key prefix.
                                    0x3, 1, 2, 6,   // 0-byte key suffix.
                                    0x3, 4, 5, 6,   // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 18,    // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new Block(bytes, 0, bytes.Length);
            var expected = new byte[] {1, 2, 4};
            foreach (var p in block.Find(new byte[] {1, 2, 3, 4})) {
                Assert.True(CompareByteArray(p.Key, 0, expected, 0, p.Key.Length));
                break;
            }
        }
    }
}
