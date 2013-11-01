using System;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {
    [TestFixture]
    public class BatchTests {
        [Test]
        public void TestBatchWriter() {
            Batch batch = new Batch();
            Assert.IsTrue(batch.IsEmpty());

            Slice[] pairs = new Slice[] {
                Utils.Slice("foo"), Utils.Slice("bar"),
                Utils.Slice("baz"), Utils.Slice("quux")
            };

            int i;
            for (i = 0; i < pairs.Length; i += 2) {
                batch.Put(pairs[i], pairs[i + 1]);
            }

            i = 0;
            foreach (IKeyValuePair kv in batch.Pairs()) {
                Assert.IsTrue(pairs[i].Equals(kv.Key));
                Assert.IsTrue(pairs[i + 1].Equals(kv.Value));
                Assert.IsFalse(kv.IsDeleted);
                i += 2;
            }
        }

        [Test]
        public void TestBatchWithDeletes() {
            Batch batch = new Batch();
            Assert.IsTrue(batch.IsEmpty());

            Slice[] pairs = new Slice[] {
                Utils.Slice("foo"), Utils.Slice("bar"),
                Utils.Slice("baz"), null,
                Utils.Slice("quux"), Utils.Slice("quuux")
            };

            int i;
            for (i = 0; i < pairs.Length; i += 2) {
                if (pairs[i + 1] == null) {
                    batch.Delete(pairs[i]);
                } else {
                    batch.Put(pairs[i], pairs[i + 1]);
                }
            }

            i = 0;
            foreach (IKeyValuePair kv in batch.Pairs()) {
                Assert.IsTrue(pairs[i].Equals(kv.Key));

                if (pairs[i + 1] == null) {
                    Assert.IsTrue(kv.IsDeleted);
                } else {
                    Assert.IsTrue(pairs[i + 1].Equals(kv.Value));
                    Assert.IsFalse(kv.IsDeleted);
                }
                i += 2;
            }
        }
    }
}

