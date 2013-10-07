using System;
using System.IO;

namespace TheFactory.Datastore.Helpers {
    public static class StreamExtensions {
        private static byte[] buf = new byte[4];
        public static UInt32 ReadInt(this Stream stream) {
            var count = stream.Read(buf, 0, 4);
            if (count < 4) {
                throw new InvalidOperationException();
            }
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(buf);
            }
            return BitConverter.ToUInt32(buf, 0);
        }
    }

    public static class ByteArrayExtensions {
        public static int CompareKey(this byte[] x, byte[] y) {
            for (var i = 0; i < x.Length && i < y.Length; i++) {
                if (x[i] < y[i]) {
                    return -1;
                } else if (x[i] > y[i]) {
                    return 1;
                }
            }

            if (x.Length < y.Length) {
                return -1;
            } else if (x.Length > y.Length) {
                return 1;
            }

            return 0;
        }
    }
}
