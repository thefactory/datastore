package com.thefactory.datastore;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BlockWriter {
    private TabletOptions opts;

    private Packer packer;

    private ByteArrayOutputStream buf;
    private ByteArrayOutputStream restarts;

    private byte[] prevKey;
    private byte[] firstKey;
    private int keyCount;

    public BlockWriter(TabletOptions opts) {
        this.opts = opts;
        this.buf = new ByteArrayOutputStream(2*opts.blockSize);

        MessagePack msgpack = new MessagePack();
        this.packer = msgpack.createPacker(this.buf);

        this.restarts = new ByteArrayOutputStream();
        this.keyCount = 0;
    }

	public void append(byte[] key, byte[] value) throws IOException {
        if (buf.size() == 0) {
            firstKey = Arrays.copyOf(key, key.length);
        }

        int shared = 0;

        if (keyCount % opts.keyRestartInterval == 0) {
            writeInt(restarts, buf.size());
        } else {
            shared = Utils.commonPrefix(prevKey, key);
        }

        packer.write(shared);
        packer.write(key, shared, key.length-shared);
        packer.write(value);

        this.prevKey = key;
        this.keyCount += 1;
    }

    private void writeInt(OutputStream out, int pos) throws IOException {
        out.write((byte) (pos >> 24));
        out.write((byte) (pos >> 16));
        out.write((byte) (pos >> 8));
        out.write((byte) (pos));
    }

    public int size() {
        // include 4 bytes for the restarts count
        return buf.size() + restarts.size() + 4;
    }

    public byte[] getFirstKey() {
        return firstKey;
    }

    public byte[] finish() throws IOException {
        buf.write(restarts.toByteArray());
        writeInt(buf, restarts.size() / 4);
        return buf.toByteArray();
    }

    public void reset() {
        buf.reset();
        restarts.reset();
        prevKey = null;
        firstKey = null;
        keyCount = 0;
    }
}
