package com.thefactory.datastore;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class TabletWriterTest extends TestCase {
    public void testWriteTablet() throws Exception {
        TabletWriterOptions opts = new TabletWriterOptions();
        opts.blockSize = 4096;
        opts.keyRestartInterval = 10;
        opts.useCompression = false;

        TabletWriter tw = new TabletWriter(opts);

        List<KV> kvs = new LinkedList<KV>();
        kvs.add(new KV("baz", "quux"));
        kvs.add(new KV("baz2", "quux"));
        kvs.add(new KV("baz3", "quux"));
        kvs.add(new KV("foo", "bar"));

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        tw.writeTablet(Channels.newChannel(buf), kvs.iterator());

        ByteArrayInputStream is = new ByteArrayInputStream(buf.toByteArray());

        byte[] tabletHeader = new byte[8];
        is.read(tabletHeader);

        assertArrayEquals(tabletHeader, new byte[]{
                0x0b, 0x50, 0x1e, 0x7e, 0x01, 0x00, 0x00, 0x00,
        });

        // precalculated block envelope for the above key-value pairs
        byte[] envelope = new byte[7];
        is.read(envelope);

        assertArrayEquals(envelope, new byte[] {
                -50, -93, -51, -51, -81, // checksum
                0, // type (uncompressed)
                43 // length
        });
    }

    public void testWriteCompressedTablet() throws Exception {
        TabletWriterOptions opts = new TabletWriterOptions();
        opts.blockSize = 4096;
        opts.keyRestartInterval = 10;
        opts.useCompression = true;

        TabletWriter tw = new TabletWriter(opts);

        List<KV> kvs = new LinkedList<KV>();
        kvs.add(new KV("baz", "quux"));
        kvs.add(new KV("baz2", "quux"));
        kvs.add(new KV("baz3", "quux"));
        kvs.add(new KV("foo", "bar"));

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        tw.writeTablet(Channels.newChannel(buf), kvs.iterator());

        ByteArrayInputStream is = new ByteArrayInputStream(buf.toByteArray());

        byte[] tabletHeader = new byte[8];
        is.read(tabletHeader);

        assertArrayEquals(tabletHeader, new byte[]{
                0x0b, 0x50, 0x1e, 0x7e, 0x01, 0x00, 0x00, 0x00,
        });

        // precalculated block envelope for the above key-value pairs
        byte[] envelope = new byte[7];
        is.read(envelope);

        assertArrayEquals(envelope, new byte[] {
                -50, 82, 30, -3, -40, // checksum
                1, // type (compressed)
                39 // length
        });
    }

    public void testFullCircle() throws Exception {
        MemoryTablet mem = new MemoryTablet();

        for(int i = 0; i < 1000; i++) {
            Slice k = new Slice(String.format("key04%d", i).getBytes());
            Slice v = new Slice("val".getBytes());
            Batch batch = new Batch();
            batch.put(k, v);
            mem.apply(batch);
        }

        Iterator<KV> kvs = mem.find();
        TabletWriter writer = new TabletWriter(new TabletWriterOptions());
        FileSystem fs = new MemFileSystem();
        FileManager fmgr = new FileManager("db", fs, true);
        DatastoreChannel channel = fs.create(fmgr.dbFilename("saving"));
        writer.writeTablet(channel, kvs);
        channel.close();

        DatastoreChannel tabletChannel = fs.open(fmgr.dbFilename("saving"));
        FileTablet ft = new FileTablet(tabletChannel, new TabletReaderOptions());

        int count = 0;
        Iterator<KV> it = ft.find();
        while(it.hasNext()) {
            KV kv = it.next();
            assertEquals(kv.getValue(), new Slice("val".getBytes()));
            count ++;
        }

        assertEquals(count, 1000);
    }

    public void testVerifyKeyOrder() {
        TabletWriterOptions opts = new TabletWriterOptions();
        opts.checkKeyOrder = true;

        final TabletWriter tw = new TabletWriter(opts);
        final List<KV> items = new ArrayList<KV>();

        Runnable closure = new Runnable() {
            public void run() {
                try {
                    tw.writeTablet(Channels.newChannel(new ByteArrayOutputStream()), items.iterator());
                } catch (IOException e) {
                    // Checked exceptions mean we need to explicitly handle IOException here.
                }
            }
        };

        Slice foo = new Slice("foo".getBytes());
        Slice bar = new Slice("bar".getBytes());

        // Check a descending key pair.
        items.clear();
        items.add(new KV(foo, foo));
        items.add(new KV(bar, bar));
        assertThrows(IllegalArgumentException.class, closure);

        // Check a duplicate key pair.
        items.clear();
        items.add(new KV(foo, foo));
        items.add(new KV(foo, foo));
        assertThrows(IllegalArgumentException.class, closure);
    }

    void assertThrows(Class cls, Runnable closure) {
        boolean thrown = false;

        try {
            closure.run();
        } catch (Exception e) {
            thrown = cls.equals(e.getClass());
        } finally {
            assertTrue("Missing expected IllegalArgumentException", thrown);
        }
    }
}
