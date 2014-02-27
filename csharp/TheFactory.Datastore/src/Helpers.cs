using System;
using System.IO;
using System.Text;

namespace TheFactory.Datastore.Helpers {
    public static class StreamExtensions {
        public static byte[] ReadBytes(this Stream stream, int count) {
            var buf = new byte[count];
            var read = stream.Read(buf, 0, count);
            while (read < count) {
                var r = stream.Read(buf, read, count - read);
                if (r == 0) {
                    throw new InvalidOperationException();
                }
                read += r;
            }
            return buf;
        }

        private static byte[] ReadNumber(this Stream stream, int width) {
            var buf = ReadBytes(stream, width);
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(buf);
            }
            return buf;
        }

        public static UInt32 ReadInt(this Stream stream) {
            var bytes = ReadNumber(stream, 4);
            return BitConverter.ToUInt32(bytes, 0);
        }

        public static UInt16 ReadShort(this Stream stream) {
            var bytes = ReadNumber(stream, 2);
            return BitConverter.ToUInt16(bytes, 0);
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
                var str = Encoding.UTF8.GetString(key, 0, key.Length);
                return String.Format("{0} ({1})", raw, str);
            } catch (ArgumentException) {
                return raw;
            }
        }
    }

    public static class ULongExtensions {
        public static byte[] ToMsgPackUInt64(this ulong val) {
            // Need to be able to pack any int to msgpack uint64 for footer.
            var buf = BitConverter.GetBytes((UInt64)val);
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(buf);
            }
            var ret = new byte[9];
            ret[0] = 0xcf;  // msgpack uint64 type.
            Buffer.BlockCopy(buf, 0, ret, 1, buf.Length);
            return ret;
        }
    }

    public static class UIntExtensions {
        public static byte[] ToNetworkBytes(this uint val) {
            // BinaryWriter's Write(UInt32) writes little-endian.
            var buf = BitConverter.GetBytes((UInt32)val);
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(buf);
            }
            return buf;
        }
    }
}
