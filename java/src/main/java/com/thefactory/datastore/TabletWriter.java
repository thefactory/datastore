package com.thefactory.datastore;

import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.zip.CRC32;

public class TabletWriter {
    TabletWriterOptions opts;

    public TabletWriter(TabletWriterOptions opts) {
        this.opts = opts;
    }

    public void writeTablet(WritableByteChannel out, Iterator<KV> kvs) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        int headLen = flush(out, writeHeader(buf, opts));

        Deque<IndexRecord> dataBlocks = writeDataBlocks(out, kvs, headLen, opts);

        int metaIndexLen = flush(out, writeIndex(buf, TabletConstants.META_INDEX_MAGIC, new LinkedList<IndexRecord>()));
        int dataIndexLen = flush(out, writeIndex(buf, TabletConstants.DATA_INDEX_MAGIC, dataBlocks));

        IndexRecord lastBlock = dataBlocks.getLast();
        long metaPos = lastBlock.offset + lastBlock.length;

        BlockHandle metaIndexHandle = new BlockHandle(metaPos, metaIndexLen);
        BlockHandle dataIndexHandle = new BlockHandle(metaPos + metaIndexLen, dataIndexLen);

        flush(out, writeFooter(buf, metaIndexHandle, dataIndexHandle));
    }

    /* flush the data in buf to out, resetting buf and returning bytes written */
    private int flush(WritableByteChannel out, ByteArrayOutputStream buf) throws IOException {
        out.write(ByteBuffer.wrap(buf.toByteArray()));

        int count = buf.size();
        buf.reset();
        return count;
    }

    private ByteArrayOutputStream writeHeader(ByteArrayOutputStream out, TabletWriterOptions opts) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);

        dos.writeInt(TabletConstants.TABLET_MAGIC);

        // 0x01: prefix-compressed blocks
        // 0x000000: reserved for future use
        dos.writeInt(0x01000000);

        return out;
    }

    private Deque<IndexRecord> writeDataBlocks(WritableByteChannel out, Iterator<KV> kvs, int pos, TabletWriterOptions opts) throws IOException {
        Deque<IndexRecord> index = new LinkedList<IndexRecord>();
        BlockWriter bw = new BlockWriter(opts);

        while (kvs.hasNext()) {
            KV kv = kvs.next();
            bw.append(kv.getKeyBytes(), kv.getValueBytes());

            if (bw.size() > opts.blockSize) {
                index.add(flushBlock(out, pos, bw, opts));
                pos += index.getLast().length;
            }
        }

        if (bw.getFirstKey() != null) {
            index.add(flushBlock(out, pos, bw, opts));
            pos += index.getLast().length;
        }

        return index;
    }

    private IndexRecord flushBlock(WritableByteChannel out, int pos, BlockWriter bw, TabletWriterOptions opts) throws IOException {
        byte[] firstKey = bw.getFirstKey();
        byte[] data = bw.finish();
        byte blockFlags = 0x00; // uncompressed block

        if (opts.useCompression) {
            byte[] compressed = Snappy.compress(data);
            if (compressed.length < data.length) {
                blockFlags = 0x01;
                data = compressed;
            }
        }

        // write the block envelope: checksum, flags, and length
        ByteArrayOutputStream env = new ByteArrayOutputStream(10 + firstKey.length);
        DataOutput dos = new DataOutputStream(env);

        // envelope is: checksum, block flags, length
        Msgpack.writeUint(dos, getChecksum(data));
        Msgpack.writeUint(dos, blockFlags);

        Msgpack.writeUint(dos, data.length);

        out.write(ByteBuffer.wrap(env.toByteArray()));
        out.write(ByteBuffer.wrap(data));

        bw.reset();

        return new IndexRecord(pos, env.size() + data.length, firstKey);
    }

    private long getChecksum(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();
    }

    private ByteArrayOutputStream writeIndex(ByteArrayOutputStream out, int magic, Deque<IndexRecord> recs) throws IOException {
        DataOutput dos = new DataOutputStream(out);
        dos.writeInt(magic);

        for (IndexRecord rec: recs) {
            Msgpack.writeUint(dos, rec.offset);
            Msgpack.writeUint(dos, rec.length);
            Msgpack.writeRaw(dos, rec.name);
        }

        return out;
    }

    private ByteArrayOutputStream writeFooter(ByteArrayOutputStream out, BlockHandle metaIndexHandle, BlockHandle dataIndexHandle) throws IOException {
        DataOutput dos = new DataOutputStream(out);
        Msgpack.writeUint64(dos, metaIndexHandle.offset);
        Msgpack.writeUint64(dos, metaIndexHandle.length);
        Msgpack.writeUint64(dos, dataIndexHandle.offset);
        Msgpack.writeUint64(dos, dataIndexHandle.length);
        dos.writeInt(TabletConstants.TABLET_MAGIC);

        return out;
    }

    public class BlockHandle {
        public BlockHandle(long offset, long length) {
            this.offset = offset;
            this.length = length;
        }

        long offset;
        long length;
    }

    public class IndexRecord {
        public IndexRecord(long offset, int length, byte[] name) {
            this.offset = offset;
            this.length = length;
            this.name = name;
        }

        long offset;
        int length;
        byte[] name;
    }
}
