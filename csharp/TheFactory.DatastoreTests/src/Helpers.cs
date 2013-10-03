using System;

namespace TheFactory.DatastoreTests {
    public static class ByteArrayExtensions {
        public static bool CompareBytes(this byte[] b1, int b1Offset, byte[] b2, int b2Offset, int count) {
            for (var i = 0; i < count; i++) {
                if (b1[b1Offset + i] != b2[b2Offset + i]) {
                    return false;
                }
            }

            return true;
        }
    }
}
