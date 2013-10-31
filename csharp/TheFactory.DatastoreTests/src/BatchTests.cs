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
                StrSlice("foo"), StrSlice("bar"),
                StrSlice("baz"), StrSlice("quux")
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
                StrSlice("foo"), StrSlice("bar"),
                StrSlice("baz"), null,
                StrSlice("quux"), StrSlice("quuux")
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

        public Slice StrSlice(string str) {
            return (Slice)System.Text.Encoding.UTF8.GetBytes(str);
        }
    }
}

