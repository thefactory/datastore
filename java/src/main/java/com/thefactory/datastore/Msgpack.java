package com.thefactory.datastore;

import org.jboss.netty.buffer.ChannelBuffer;

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
    public static int writeUint(ChannelBuffer buf, long n) {
        int pos = buf.writerIndex();

        if (n <= 0x7f) {
            buf.writeByte((int)n);
        } else if (n <= 0xff) {
            buf.writeByte(MSG_UINT_8);
            buf.writeByte((int)n);
        } else if (n <= 0xffff) {
            buf.writeByte(MSG_UINT_16);
            buf.writeShort((int)n);
        } else if (n <= 0xffffffff) {
            buf.writeByte(MSG_UINT_32);
            buf.writeInt((int)n);
        } else {
            buf.writeByte(MSG_UINT_64);
            buf.writeLong(n);
        }

        return buf.writerIndex() - pos;
    }

    public static int writeRaw(ChannelBuffer buf, byte[] data) {
        int pos = buf.writerIndex();

        if (data.length < 32) {
            buf.writeByte(MSG_FIX_RAW | (byte)data.length);
        } else if (data.length < 65536) {
            buf.writeByte(MSG_RAW_16);
            buf.writeShort(data.length);
        } else {
            buf.writeByte(MSG_RAW_32);
            buf.writeInt(data.length);
        }

        buf.writeBytes(data);

        return buf.writerIndex() - pos;
    }

    public static void writeUint64(ChannelBuffer buf, long num) {
        buf.writeByte(MSG_UINT_64);
        buf.writeLong(num);
    }
}
