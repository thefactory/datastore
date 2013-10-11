using System;
using System.IO;
using System.Text;

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

        public static void WriteInt(this Stream stream, UInt32 val) {
            // Always write in network byte order.
            var bytes = BitConverter.GetBytes(val);
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(bytes);
            }
            stream.Write(bytes, 0, 4);
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

        public static int CommonBytes(this byte[] x, byte[] y) {
            if (y == null) {
                return 0;
            }

            var length = x.Length < y.Length ? x.Length : y.Length;

            int count;
            for (count = 0; count < length; count++) {
                if (x[count] != y[count]) {
                    break;
                }
            }
            return count;
        }

        public static string StringifyKey(this byte[] key) {
            var raw = BitConverter.ToString(key);
            try {
                var str = Encoding.UTF8.GetString(key);
                return String.Format("{0} ({1})", raw, str);
            } catch (ArgumentException) {
                return raw;
            }
        }
    }
}
