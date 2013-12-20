package com.thefactory.datastore;

import junit.framework.TestCase;
import java.util.Iterator;
import java.io.IOException;
import java.util.List;
import org.xerial.snappy.Snappy;

import static org.junit.Assert.assertArrayEquals;

public class TabletReaderTest extends TestCase {

    private static byte[] compressBlock(byte[] data) throws IOException{
        // byte[] buf = new byte[Snappy.maxCompressedLength(data.length)];
        byte[] compressed = Snappy.compress(data);

        byte[] header = new byte[] {0, 1, (byte) compressed.length};
        byte[] ret = new byte[header.length + compressed.length];
        System.arraycopy(header, 0, ret, 0, header.length);
        System.arraycopy(compressed, 0, ret, header.length, compressed.length);

        return ret;
    }


    public void testTabletLoadBlockOneUncompressed() throws IOException{
        // Simple block with header.
        byte[] bytes = new byte[] {
            0,                          // H: checksum.
            0,                          // H: type (uncompressed).
            13,                         // H: length.
            0,                          // 0-byte key prefix.
            (byte)0xa3, 1, 2, 3,        // 3-byte key suffix.
            (byte)0xa3, 4, 5, 6,        // 3-byte value.
            0, 0, 0, 0                  // no restart indexes.
        };

        Slice in = new Slice(bytes);
        TabletReader tabletReader = new TabletReader();
        BlockReader block = tabletReader.readBlock(in);
        int count = 0;
        Iterator<KV> kvs = block.find();
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), new Slice(new byte[] {1, 2, 3}).toString());
            assertEquals(kv.getValue().toString(), new Slice(new byte[] {4, 5, 6}).toString());
            count += 1;
        }
        assertEquals(count, 1);
    }

    public void testTabletLoadBlockOneSnappy() throws IOException {
        byte[] bytes = new byte[] {
            0,                            // 0-byte key prefix.
            (byte)0xa3, 1, 2, 3,          // 3-byte key suffix.
            (byte)0xa3, 4, 5, 6,          // 3-byte value.
            0, 0, 0, 0                    // no restart indexes.
        };
        Slice in = new Slice(compressBlock(bytes));
        BlockReader block = new TabletReader().readBlock(in);
        int count = 0;
        Iterator<KV> kvs = block.find();
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), new Slice(new byte[] {1, 2, 3}).toString());
            assertEquals(kv.getValue().toString(), new Slice(new byte[] {4, 5, 6}).toString());
            count += 1;
        }
        assertEquals(count, 1);
    }

    public void testTabletFooterLoad() throws IOException {
        byte[] bytes = new byte[] {
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 0,             // MetaIndexOffset msgpack uint64.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 0,             // MetaIndexLength msgpack uint64.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 0,             // DataIndexOffset msgpack uint64.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 0,             // DataIndexLength msgpack uint64.
            (byte)0x0b, (byte)0x50, (byte)0x1e, (byte)0x7e  // Tablet magic (0x0b501e7e).
        };

        TabletReader.TabletFooter footer = new TabletReader().readFooter(new Slice(bytes));

        assertEquals(footer.metaIndexOffset, 0);
        assertEquals(footer.metaIndexLength, 0);
        assertEquals(footer.dataIndexOffset, 0);
        assertEquals(footer.dataIndexLength, 0);
    }

    public void testTabletFooterPaddedLoad() throws IOException {
        byte[] bytes = new byte[] {
            0,                            // MetaIndexOffset (msgpack fixpos)
            (byte)0xcc, 0,                // MetaIndexLength (msgpack uint8)
            (byte)0xcd, 0, 0,             // DataIndexOffset (msgpack uint16)
            (byte)0xce, 0, 0, 0, 0,       // DataIndexLength (msgpack uint32)
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // padding
            (byte)0x0b, (byte)0x50, (byte)0x1e, (byte)0x7e  // Tablet magic (0x0b501e7e).
        };

        TabletReader.TabletFooter footer = new TabletReader().readFooter(new Slice(bytes));

        assertEquals(footer.metaIndexOffset, 0);
        assertEquals(footer.metaIndexLength, 0);
        assertEquals(footer.dataIndexOffset, 0);
        assertEquals(footer.dataIndexLength, 0);
    }

    public void testTabletFooterLoadBadMagic() {
        byte[] bytes = new byte[] {
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // MetaIndexOffset msgpack uint64.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // MetaIndexLength msgpack uint64.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // DataIndexOffset msgpack uint64.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 0,  // DataIndexLength msgpack uint64.
            0, 0, 0, 0                           // Bad tablet magic.
        };
        try {
            TabletReader.TabletFooter footer = new TabletReader().readFooter(new Slice(bytes));
            assert false;
        } catch (Exception e) {
            assertEquals(e.getClass(), IOException.class);
        }
    }

    public void testTabletLoadIndexSimple() throws IOException {
        byte[] bytes = new byte[] {
            0, 0, 0, 0,             // magic (0).
            0, 10, (byte)0xa1, 1,   // offset: 0, length: 10, data: 1.
            10, 10, (byte)0xa1, 2,  // offset: 10, length: 10, data: 2.
        };

        List<TabletReader.TabletIndexRecord> index =  new TabletReader().readIndex(new Slice(bytes), 10, 0);

        assertEquals(index.size(), 2);
        assertEquals(index.get(0).offset, bytes[4]);
        assertEquals(index.get(0).length, bytes[5]);
        assertEquals(index.get(0).data.getAt(0), bytes[7]);
        assertEquals(index.get(1).offset, bytes[8]);
        assertEquals(index.get(1).length, bytes[9]);
        assertEquals(index.get(1).data.getAt(0), bytes[11]);
    }

    public void testTabletLoadIndexBadMagic() {
        byte[] bytes = new byte[] {
            0, 0, 0, 0,             // magic (0).
            0, 10, (byte)0xa1, 1,   // offset: 0, length: 10, data: 1.
            10, 10, (byte)0xa1, 2,  // offset: 10, length: 10, data: 2.
        };
        try {
            List<TabletReader.TabletIndexRecord> index =  new TabletReader().readIndex(new Slice(bytes), 10, 1);
            assert false;
        } catch (Exception e) {
            assertEquals(e.getClass(), IOException.class);
        }
    }
}