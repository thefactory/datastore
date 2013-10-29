using System;

namespace TheFactory.Datastore {
    public class Utils {
        public static int ToUInt32(Slice slice) {
            // unpack the first 4 bytes of slice as a big-endian int
            return slice[0] << 24 | slice[1] << 16 | slice[2] << 8 | slice[3];
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

