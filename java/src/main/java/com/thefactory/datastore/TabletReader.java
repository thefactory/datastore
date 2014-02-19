package com.thefactory.datastore;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.channels.ReadableByteChannel;
import org.xerial.snappy.Snappy;
import java.util.zip.CRC32;

public class TabletReader {

    public TabletHeader readHeader(Slice in) throws IOException {
        return new TabletHeader(in);
    }

    public List<TabletIndexRecord> readIndex(Slice in, long length, long magic) throws IOException {
        long offset = in.getOffset();

        long m = in.readInt();
        if (m != magic) {
            throw new IOException(String.format("bad index magic {%02X}, expected {%02X}", m, magic));
        }

        ArrayList<TabletIndexRecord> ret = new ArrayList<TabletIndexRecord>();

        while (in.getOffset() < offset + length) {
            ret.add(new TabletIndexRecord(in));
        }

        return ret;
    }

    public BlockReader readBlock(Slice in) throws IOException {
        return readBlock(in, false);
    }

    public BlockReader readBlock(Slice in, boolean verifyChecksum) throws IOException {
        TabletBlockData blockData = new TabletBlockData(in);

        if(verifyChecksum && blockData.info.checksum != 0 && blockData.info.checksum != blockData.checksum) {
            throw new IOException("bad block checksum");
        }

        return new BlockReader(new Slice(blockData.data));
    }    

    public TabletFooter readFooter(Slice in) throws IOException {
        return new TabletFooter(in);
    }    

    private enum BlockType {
        DATA, META
    }

    public static class TabletIndexRecord {
        public final long offset;
        public final int length;
        public final Slice data;

        public TabletIndexRecord(Slice in) throws IOException {
            this.offset = Msgpack.readUint(in);
            this.length = (int) Msgpack.readUint(in);
            long len = Msgpack.readRawLength(in);
            this.data = in.subslice(0, (int)len);
            in.forward((int)len);
        }
    }

    public static class TabletHeader {
        public final long magic;
        public final int version;

        public TabletHeader(Slice in) throws IOException {
            magic = in.readInt();
            if (magic != TabletConstants.TABLET_MAGIC) {
                throw new IOException(String.format("bad tablet magic {0:%02X}", magic));
            }

            version = in.readByte();
            if (version < 1) {
                throw new IOException(String.format("bad version"));                
            }
        }
    }

    public static class TabletFooter {
        public final long metaIndexOffset;
        public final long metaIndexLength;
        public final long dataIndexOffset;
        public final long dataIndexLength;

        public TabletFooter(Slice in) throws IOException {
            if (in.getLength() != 40) {
                throw new IOException(String.format("Internal error: tablet footer length != 40 (%d)", in.getLength()));
            }     
            this.metaIndexOffset = Msgpack.readUint(in);
            this.metaIndexLength = Msgpack.readUint(in);
            this.dataIndexOffset = Msgpack.readUint(in);
            this.dataIndexLength = Msgpack.readUint(in);
            long magic = Utils.toUInt32(in.subslice(-4)); 
            if (magic != TabletConstants.TABLET_MAGIC) {
                throw new IOException(String.format("bad tablet magic {0:%02X}", magic));
            }
        }
    }

    public static class TabletBlockInfo {
        public final BlockType type;
        public final long checksum;
        public final int length;
        public final boolean isCompressed;

        public TabletBlockInfo(Slice in) throws IOException {
            // type: 0b000000TC
            // C: block compression: 0 = None, 1 = Snappy
            // T: block type: 0 = Data block, 1 = Metadata block
            //
            this.checksum =  Msgpack.readUint(in);
            int t = (int) Msgpack.readUint(in);
            this.type = (t & (1 << 1)) == 0 ? BlockType.DATA : BlockType.META;
            this.length = (int) Msgpack.readUint(in);
            this.isCompressed = (t & 1) == 1;
        }
    }    

    public static class TabletBlockData {
        public final TabletBlockInfo info;
        public final byte[] data;
        public long checksum;

        public TabletBlockData(Slice in) throws IOException {
            this.info = new TabletBlockInfo(in);

            byte[] bytes = in.toArray();

            if (info.isCompressed) {
                this.data = Snappy.uncompress(bytes);
            } else {
                this.data = bytes;
            }

            CRC32 crc32 = new CRC32();
            crc32.update(bytes);
            checksum = crc32.getValue();
        }
    }
}
