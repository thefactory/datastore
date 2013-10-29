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

        [Test]
        public void TestSubslice() {
            var s0 = (Slice)new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

            /* one-argument slicing */
            Assert.True(Slice.Compare(s0.Subslice(0), s0) == 0);
            Assert.True(Slice.Compare(s0.Subslice(2), (Slice)new byte[] { 2, 3, 4, 5, 6, 7, 8, 9 }) == 0);
            Assert.True(Slice.Compare(s0.Subslice(-4), (Slice)new byte[] { 6, 7, 8, 9 }) == 0);

            /* two-argument slicing */
            Assert.True(Slice.Compare(s0.Subslice(0, 4), (Slice)new byte[] { 0, 1, 2, 3 }) == 0);
            Assert.True(Slice.Compare(s0.Subslice(2, 4), (Slice)new byte[] { 2, 3, 4, 5 }) == 0);
            Assert.True(Slice.Compare(s0.Subslice(-4, 2), (Slice)new byte[] { 6, 7 }) == 0);
        }
    }
}

