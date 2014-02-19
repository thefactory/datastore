package com.thefactory.datastore;

public class Utils {

    public static long toUInt32(Slice slice) {
        return (long)(slice.getAt(0) << 24 | slice.getAt(1) << 16 | slice.getAt(2) << 8 | slice.getAt(3)) & 0xffffffff;
    }

    public static int commonPrefix(byte[] bin1, byte[] bin2) {
        int num = Math.min(bin1.length, bin2.length);
        int count = 0;

        for (int i=0; i<num; i++) {
            if (bin1[i] == bin2[i]) {
                count++;
            } else {
                break;
            }
        }

        return count;
    }
}
