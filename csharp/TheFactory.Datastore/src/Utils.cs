using System;
using System.IO;

namespace TheFactory.Datastore {
    public class Utils {
        public static UInt32 ToUInt32(Slice slice) {
            // unpack the first 4 bytes of slice as a big-endian int
            return (UInt32)(slice[0] << 24 | slice[1] << 16 | slice[2] << 8 | slice[3]);
        }

        public static int WriteUInt32(Stream stream, UInt32 num) {
            // write a uint32 to stream in big-endian byte order
            byte[] buf = new byte[4];
            buf[0] = (byte)(num >> 24);
            buf[1] = (byte)(num >> 16);
            buf[2] = (byte)(num >> 8);
            buf[3] = (byte)num;

            stream.Write(buf, 0, 4);
            return 4;
        }

        public static int WriteUInt16(Stream stream, UInt16 num) {
            // write a uint16 to stream in big-endian byte order
            byte[] buf = new byte[2];
            buf[0] = (byte)(num >> 8);
            buf[1] = (byte)num;

            stream.Write(buf, 0, 2);
            return 2;
        }

        public static int Search(int n, Func<int,bool> f) {
            // binary search: returns the first index in [0..n-1] where f(index) == true
            // if there is no such index, Search returns n
            int i = 0;
            int j = n;
            while (i < j) {
                int h = i + ((j - i) / 2);
                if (!f(h)) {
                    i = h + 1;
                } else {
                    j = h;
                }
            }
            return i;
        }
    }
}

