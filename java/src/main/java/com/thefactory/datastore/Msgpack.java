package com.thefactory.datastore;

import java.io.DataOutput;
import java.io.IOException;

public class Msgpack {
    public static int MSG_FIX_POS = 0x00;
    public static int MSG_UINT_8 = 0xcc;
    public static int MSG_UINT_16 = 0xcd;
    public static int MSG_UINT_32 = 0xce;
    public static int MSG_UINT_64 = 0xcf;

    public static int MSG_FIX_RAW = 0xa0;
    public static int MSG_RAW_16 = 0xda;
    public static int MSG_RAW_32 = 0xdb;

    /* lightweight msgpack routines */
    public static int writeUint(DataOutput out, long n) throws IOException {
        if (n <= 0x7fL) {
            out.write((int) n);
            return 1;
        } else if (n <= 0xffL) {
            out.write(MSG_UINT_8);
            out.write((int) n);
            return 2;
        } else if (n <= 0xffffL) {
            out.write(MSG_UINT_16);
            out.writeShort((int) n);
            return 3;
        } else if (n <= 0xffffffffL) {
            out.writeByte(MSG_UINT_32);
            out.writeInt((int) n);
            return 5;
        } else {
            out.writeByte(MSG_UINT_64);
            out.writeLong(n);
            return 9;
        }
    }

    public static int writeRaw(DataOutput out, byte[] data) throws IOException {
        int n = 0;
        if (data.length < 32) {
            out.write(MSG_FIX_RAW | (byte)data.length);
            n = 1;
        } else if (data.length < 65536) {
            out.write(MSG_RAW_16);
            out.writeShort(data.length);
            n = 3;
        } else {
            out.write(MSG_RAW_32);
            out.writeShort(data.length);
            n = 5;
        }

        out.write(data);

        return n + data.length;
    }

    public static void writeUint64(DataOutput out, long num) throws IOException {
        out.writeByte(MSG_UINT_64);
        out.writeLong(num);
    }
}
