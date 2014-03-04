using System;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {
    [TestFixture]
    public class BlockWriterTests {
        [Test]
        public void TestBlockWriterOnePairUncompressed() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // restart index at 0.
                                    0, 0, 0, 1};    // 1 restart index.
            var writer = new BlockWriter(10);
            var firstKey = new byte[] {1, 2, 3};
            writer.Append((Slice)firstKey, (Slice)(new byte[] {4, 5, 6}));
            var output = writer.Finish();
            Assert.True(output.FirstKey.Equals((Slice)(new byte[] { 1, 2, 3 })));
            Assert.True(((byte[])output.Buffer).CompareBytes(0, bytes, 0, bytes.Length));
        }

        [Test]
        public void TestBlockWriterOnePairUncompressedReset() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // restart index at 0.
                                    0, 0, 0, 1};    // 1 restart index.
            var writer = new BlockWriter(10);
            var firstKey = new byte[] {1, 2, 3};
            writer.Append((Slice)firstKey, (Slice)(new byte[] {4, 5, 6}));
            var output = writer.Finish();
            Assert.True(output.FirstKey.Equals((Slice)firstKey));
            Assert.True(((byte[])output.Buffer).CompareBytes(0, bytes, 0, bytes.Length));
            writer.Reset();
            writer.Append((Slice)firstKey, (Slice)(new byte[] {4, 5, 6}));
            output = writer.Finish();
            Assert.True(output.FirstKey.Equals((Slice)firstKey));
            Assert.True(((byte[])output.Buffer).CompareBytes(0, bytes, 0, bytes.Length));
        }

        [Test]
        public void TestBlockWriterManyPairsUncompressedWithSize() {
            var pairs = new byte[][] {
                new byte[] {1, 2, 3}, new byte[] {4, 5, 6},
                new byte[] {2, 3, 4}, new byte[] {4, 5, 6},
                new byte[] {3, 4, 5}, new byte[] {4, 5, 6},
                new byte[] {4, 5, 6}, new byte[] {4, 5, 6}
            };
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 2, 3, 4,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 3, 4, 5,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 4, 5, 6,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // restart index at 0.
                0, 0, 0, 1};    // 1 restart index.
            var writer = new BlockWriter(10);
            var firstKey = pairs[0];
            var sizeCount = 1;
            for (var i = 0; i < pairs.Length; i += 2) {
                writer.Append((Slice)pairs[i], (Slice)(pairs[i + 1]));
                Assert.True(writer.Size == sizeCount * 9 + 4 + 4);
                sizeCount += 1;
            }
            var output = writer.Finish();
            Assert.True(output.FirstKey.Equals((Slice)firstKey));
            Assert.True(((byte[])output.Buffer).CompareBytes(0, bytes, 0, bytes.Length));
        }

        [Test]
        public void TestBlockWriterManyPairsUncompressedWithPrefixes() {
            var pairs = new byte[][] {
                new byte[] {1, 1, 1}, new byte[] {4, 5, 6},
                new byte[] {1, 1, 2}, new byte[] {4, 5, 6},
                new byte[] {1, 1, 3}, new byte[] {4, 5, 6},
                new byte[] {1, 1, 4}, new byte[] {4, 5, 6}
            };
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 1, 1,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    2,              // 0-byte key prefix.
                                    0xa1, 2,        // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    2,              // 2-byte key prefix.
                                    0xa1, 3,        // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    2,              // 2-byte key prefix.
                                    0xa1, 4,        // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // restart index at 0.
                                    0, 0, 0, 1};    // 1 restart index.
            var writer = new BlockWriter(10);
            var firstKey = pairs[0];
            for (var i = 0; i < pairs.Length; i += 2) {
                writer.Append((Slice)pairs[i], (Slice)(pairs[i + 1]));
            }
            var output = writer.Finish();
            Assert.True(output.FirstKey.Equals((Slice)firstKey));
            Assert.True(((byte[])output.Buffer).CompareBytes(0, bytes, 0, bytes.Length));
        }

        [Test]
        public void TestBlockWriterManyPairsUncompressedWithRestarts() {
            var pairs = new byte[][] {
                new byte[] {1, 2, 3}, new byte[] {4, 5, 6},
                new byte[] {2, 3, 4}, new byte[] {4, 5, 6}
            };
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 2, 3, 4,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // restart index at 0.
                                    0, 0, 0, 9,     // restart index at 0.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var writer = new BlockWriter(1);
            var firstKey = pairs[0];
            for (var i = 0; i < pairs.Length; i += 2) {
                writer.Append((Slice)pairs[i], (Slice)(pairs[i + 1]));
            }
            var output = writer.Finish();
            Assert.True(output.FirstKey.Equals((Slice)firstKey));
            Assert.True(((byte[])output.Buffer).CompareBytes(0, bytes, 0, bytes.Length));
        }

        [Test]
        public void TestBlockWriterManyPairsUncompressedWithPrefixesAndRestarts() {
            var pairs = new byte[][] {
                new byte[] {1, 1, 1}, new byte[] {4, 5, 6},
                new byte[] {1, 1, 2}, new byte[] {4, 5, 6},
                new byte[] {1, 1, 3}, new byte[] {4, 5, 6},
                new byte[] {1, 1, 4}, new byte[] {4, 5, 6}
            };
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 1, 1,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    2,              // 0-byte key prefix.
                                    0xa1, 2,        // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 1, 1, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    2,              // 2-byte key prefix.
                                    0xa1, 4,        // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // restart index at 0.
                                    0, 0, 0, 16,    // restart index at 16.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var writer = new BlockWriter(2);
            var firstKey = pairs[0];
            for (var i = 0; i < pairs.Length; i += 2) {
                writer.Append((Slice)pairs[i], (Slice)(pairs[i + 1]));
            }
            var output = writer.Finish();
            Assert.True(output.FirstKey.Equals((Slice)firstKey));
            Assert.True(((byte[])output.Buffer).CompareBytes(0, bytes, 0, bytes.Length));
        }
    }

    [TestFixture]
    public class BlockTests {
        [Test]
        public void TestBlockIterateOne() {
            // Simple block segment without key prefix.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new BlockReader((Slice)bytes);
            var count = 0;
            foreach (var p in block.Find(Slice.Empty)) {
                Assert.True(p.Key.Equals((Slice)new byte[] { 1, 2, 3 }));
                Assert.True(p.Value.Equals((Slice)new byte[] { 4, 5, 6 }));
                count += 1;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestBlockIterateManyNoPrefix() {
            // Simple block segment without key prefixes.
            // Note: we don't enforce unique keys.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new BlockReader((Slice)bytes);
            var count = 0;
            foreach (var p in block.Find(Slice.Empty)) {
                Assert.True(p.Key.Equals((Slice)(new byte[] { 1, 2, 3 })));
                Assert.True(p.Value.Equals((Slice)(new byte[] { 4, 5, 6 })));
                count += 1;
            }
            Assert.True(count == 2);
        }

        [Test]
        public void TestBlockIterateManyWithPrefix() {
            // Simple block segment without key prefixes.
            // Note: we don't enforce unique keys.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    1,              // 1-byte key prefix.
                                    0xa2, 2, 3,     // 2-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    2,              // 2-byte key prefix.
                                    0xa1, 3,        // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    3,              // 3-byte key prefix.
                                    0xa0,           // 0-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new BlockReader((Slice)bytes);
            var count = 0;
            foreach (var p in block.Find(Slice.Empty)) {
                Assert.True(p.Key.Equals((Slice)new byte[] { 1, 2, 3 }));
                Assert.True(p.Value.Equals((Slice)new byte[] { 4, 5, 6 }));
                count += 1;
            }
            Assert.True(count == 4);
        }

        [Test]
        public void TestBlockWithRestarts() {
            // Simple block segment without key prefixes.
            // Note: we don't enforce unique keys.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 9,     // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new BlockReader((Slice)bytes);
            var count = 0;
            foreach (var p in block.Find(Slice.Empty)) {
                Assert.True(p.Key.Equals((Slice)new byte[] { 1, 2, 3 }));
                Assert.True(p.Value.Equals((Slice)new byte[] { 4, 5, 6 }));
                count += 1;
            }
            Assert.True(count == 2);
        }

        [Test]
        public void TestBlockMidStream() {
            // Simple block starts and ends mid-[tablet]-stream.
            // This is really three data blocks in a row.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // no restart indexes.
                                    0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // no restart indexes.
                                    0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new BlockReader(new Slice(bytes, 13, 13));  // read the middle block.
            var count = 0;
            foreach (var p in block.Find(Slice.Empty)) {
                Assert.True(p.Key.Equals((Slice)new byte[] { 1, 2, 3 }));
                Assert.True(p.Value.Equals((Slice)new byte[] { 4, 5, 6 }));
                count += 1;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestBlockFindOnRestart() {
            // Simple block segment without key prefixes.
            // Note: we don't enforce unique keys.
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 2, 3, 4,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 3, 4, 5,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 9,     // second restart.
                                    0, 0, 0, 18,    // third restart.
                                    0, 0, 0, 3};    // 3 restart indexes.
            var block = new BlockReader((Slice)bytes);
            var term = (Slice)(new byte[] {2, 3, 4});
            var count = 0;
            foreach (var p in block.Find(term)) {
                count += 1;
                Assert.True(p.Key.Equals(term));
                break;
            }

            Assert.True(count == 1);
        }

        [Test]
        public void TestBlockFindOffRestart() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 4,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 5,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 6,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 18,    // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new BlockReader((Slice)bytes);
            var term = (Slice)(new byte[] {1, 2, 4});
            var count = 0;
            foreach (var p in block.Find(term)) {
                count += 1;
                Assert.True(p.Key.Equals(term));
                break;
            }
            Assert.True(count == 1);
        }

        [Test]
        public void TestBlockFindNoRestarts() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    1,              // 1-byte key prefix.
                                    0xa2, 2, 4,     // 2-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    2,              // 2-byte key prefix.
                                    0xa1, 5,        // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    2,              // 3-byte key prefix.
                                    0xa1, 6,        // 0-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0};    // no restart indexes.
            var block = new BlockReader((Slice)bytes);
            var term = (Slice)(new byte[] {1, 2, 5});

            var count = 0;
            foreach (var p in block.Find(term)) {
                count += 1;
                Assert.True(Slice.Compare(p.Key, term) >= 0);
            }

            Assert.True(count == 2);
        }

        [Test]
        public void TestBlockFindUnmatchedBefore() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 1-byte key prefix.
                                    0xa3, 1, 2, 4,  // 2-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 2-byte key prefix.
                                    0xa3, 1, 2, 5,  // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 3-byte key prefix.
                                    0xa3, 1, 2, 6,  // 0-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 18,    // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new BlockReader((Slice)bytes);
            var term = (Slice)(new byte[] {0, 1, 2});

            var count = 0;
            foreach (var p in block.Find(term)) {
                count += 1;
                Assert.True(Slice.Compare(p.Key, term) > 0);
            }

            // ensure all results were yielded
            Assert.True(count == 4);
        }

        [Test]
        public void TestBlockFindUnmatchedAfter() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 1-byte key prefix.
                                    0xa3, 1, 2, 4,  // 2-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 2-byte key prefix.
                                    0xa3, 1, 2, 5,  // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 3-byte key prefix.
                                    0xa3, 1, 2, 6,  // 0-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 18,    // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new BlockReader((Slice)bytes);
            var count = 0;
            foreach (var p in block.Find((Slice)(new byte[] {2, 3, 4}))) {
                count += 1;
                break;
            }
            // Iterator returned by Find is at its end; no iteration.
            Assert.True(count == 0);
        }

        [Test]
        public void TestBlockFindUnmatchedMiddle() {
            var bytes = new byte[] {0,              // 0-byte key prefix.
                                    0xa3, 1, 2, 3,  // 3-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 1-byte key prefix.
                                    0xa3, 1, 2, 4,  // 2-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 2-byte key prefix.
                                    0xa3, 1, 2, 5,  // 1-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0,              // 3-byte key prefix.
                                    0xa3, 1, 2, 6,  // 0-byte key suffix.
                                    0xa3, 4, 5, 6,  // 3-byte value.
                                    0, 0, 0, 0,     // first restart.
                                    0, 0, 0, 18,    // second restart.
                                    0, 0, 0, 2};    // 2 restart indexes.
            var block = new BlockReader((Slice)bytes);
            var term = (Slice)(new byte[] {1, 2, 4});

            var count = 0;
            foreach (var p in block.Find((Slice)(new byte[] {1, 2, 3, 4}))) {
                count += 1;
                Assert.True(Slice.Compare(p.Key, term) == 0);
                break;
            }

            Assert.True(count == 1);
        }
    }
}
