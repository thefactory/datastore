package com.thefactory.datastore;

import junit.framework.TestCase;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.LinkedList;
import java.util.List;

public class TabletWriterTest extends TestCase {
    public void testWriteTablet() throws Exception {
        TabletOptions opts = new TabletOptions();
        opts.blockSize = 4096;
        opts.keyRestartInterval = 10;
        opts.useCompression = false;

        TabletWriter tw = new TabletWriter(opts);

        List<KV> kvs = new LinkedList<KV>();
        kvs.add(new KV("baz", "quux"));
        kvs.add(new KV("baz2", "quux"));
        kvs.add(new KV("baz3", "quux"));
        kvs.add(new KV("foo", "bar"));

        ChannelBuffer buf = ChannelBuffers.buffer(4096);
        tw.writeTablet(buf, kvs);

        // not the best test of the written data
        assertEquals(108, buf.writerIndex());
    }

    public void testWriteCompressedTablet() throws Exception {
        TabletOptions opts = new TabletOptions();
        opts.blockSize = 4096;
        opts.keyRestartInterval = 10;
        opts.useCompression = true;

        TabletWriter tw = new TabletWriter(opts);

        List<KV> kvs = new LinkedList<KV>();
        kvs.add(new KV("baz", "quux"));
        kvs.add(new KV("baz2", "quux"));
        kvs.add(new KV("baz3", "quux"));
        kvs.add(new KV("foo", "bar"));

        ChannelBuffer buf = ChannelBuffers.buffer(4096);
        tw.writeTablet(buf, kvs);

        // not the best test of the written data
        assertEquals(104, buf.writerIndex());
    }
}
