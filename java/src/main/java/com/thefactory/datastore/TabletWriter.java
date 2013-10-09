package com.thefactory.datastore;

import org.jboss.netty.buffer.ChannelBuffer;
import org.msgpack.packer.Packer;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.*;
import java.util.zip.CRC32;

public class TabletWriter {
    TabletOptions opts;

    public TabletWriter(TabletOptions opts) {
        this.opts = opts;
    }

    public void writeTablet(ChannelBuffer buf, Iterable<KV> kvs) throws IOException {
        int headLen = writeHeader(buf, opts);

        Deque<IndexRecord> dataBlocks = writeDataBlocks(buf, kvs, headLen, opts);

        int metaIndexLen = writeIndex(buf, TabletConstants.META_INDEX_MAGIC, new LinkedList<IndexRecord>());
        int dataIndexLen = writeIndex(buf, TabletConstants.DATA_INDEX_MAGIC, dataBlocks);

        IndexRecord lastBlock = dataBlocks.getLast();
        long metaPos = lastBlock.offset + lastBlock.length;

        BlockHandle metaIndexHandle = new BlockHandle(metaPos, metaIndexLen);
        BlockHandle dataIndexHandle = new BlockHandle(metaPos + metaIndexLen, dataIndexLen);

        writeFooter(buf, metaIndexHandle, dataIndexHandle);
    }

    private int writeHeader(ChannelBuffer buf, TabletOptions opts) {
        int pos = buf.writerIndex();

        buf.writeInt(TabletConstants.TABLET_MAGIC);

        // 0x01: prefix-compressed blocks
        // 0x000000: reserved for future use
        buf.writeInt(0x01000000);

        return buf.writerIndex() - pos;
    }

    private Deque<IndexRecord> writeDataBlocks(ChannelBuffer buf, Iterable<KV> kvs, int pos, TabletOptions opts) throws IOException {
        Deque<IndexRecord> index = new LinkedList<IndexRecord>();
        BlockWriter bw = new BlockWriter(opts);

        for (KV kv: kvs) {
            bw.append(kv.getKey(), kv.getValue());

            if (bw.size() > opts.blockSize) {
                index.add(flushBlock(buf, bw, opts));
            }
        }

        if (bw.getFirstKey() != null) {
            index.add(flushBlock(buf, bw, opts));
        }

        return index;
    }

    private IndexRecord flushBlock(ChannelBuffer buf, BlockWriter bw, TabletOptions opts) throws IOException {
        byte[] firstKey = bw.getFirstKey();
        byte[] data = bw.finish();
        byte blockFlags = 0x00; // uncompressed block

        int pos = buf.writerIndex();

        if (opts.useCompression) {
            byte[] compressed = Snappy.compress(data);
            if (compressed.length < data.length) {
                blockFlags = 0x01;
                data = compressed;
            }
        }

        // write checksum (not calculated for now)
        Msgpack.writeUint(buf, 0x00000000);
        Msgpack.writeUint(buf, blockFlags);

        Msgpack.writeUint(buf, data.length);
        buf.writeBytes(data);
        bw.reset();

        return new IndexRecord(pos, buf.writerIndex() - pos, firstKey);
    }

    private int writeIndex(ChannelBuffer buf, int magic, Deque<IndexRecord> recs) {
        int pos = buf.writerIndex();

        buf.writeInt(magic);

        for (IndexRecord rec: recs) {
            Msgpack.writeUint(buf, rec.offset);
            Msgpack.writeUint(buf, rec.length);
            Msgpack.writeRaw(buf, rec.name);
        }

        return buf.writerIndex() - pos;
    }

    private int writeFooter(ChannelBuffer buf, BlockHandle metaIndexHandle, BlockHandle dataIndexHandle) {
        int pos = buf.writerIndex();

        Msgpack.writeUint64(buf, metaIndexHandle.offset);
        Msgpack.writeUint64(buf, metaIndexHandle.length);
        Msgpack.writeUint64(buf, dataIndexHandle.offset);
        Msgpack.writeUint64(buf, dataIndexHandle.length);
        buf.writeInt(TabletConstants.TABLET_MAGIC);

        return buf.writerIndex() - pos;
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
