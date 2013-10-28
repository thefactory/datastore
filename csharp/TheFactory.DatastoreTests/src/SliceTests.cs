using System;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {
    [TestFixture]
    public class SliceTests
    {
        [Test]
        public void TestCompareSameLength() {
            var s0 = (Slice)new byte[] { 0 };
            var s1 = (Slice)new byte[] { 1 };

            Assert.True(Slice.Compare(s0, s1) < 0);
            Assert.True(Slice.Compare(s0, s0) == 0);
            Assert.True(Slice.Compare(s1, s0) > 0);
        }

        [Test]
        public void TestCompare() {
            var s0 = (Slice)new byte[] { 0 };
            var s1 = (Slice)new byte[] { 0, 1 };

            Assert.True(Slice.Compare(s0, s1) < 0);
            Assert.True(Slice.Compare(s0, s0) == 0);
            Assert.True(Slice.Compare(s1, s0) > 0);
        }
    }
}

