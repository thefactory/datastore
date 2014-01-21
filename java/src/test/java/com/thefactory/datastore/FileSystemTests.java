package com.thefactory.datastore;

import junit.framework.TestCase;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.io.File;

public class FileSystemTests extends TestCase {

    public void testMemFileSystem() throws Exception {
        MemFileSystem fs = new MemFileSystem();

        DatastoreChannel channel = fs.create("test1");

        assertTrue(fs.exists("test1"));
        assertFalse(fs.exists("foo"));

        channel.write(ByteBuffer.wrap(new byte[]{0, 0, 0, 1}));
        channel.close();

        ByteBuffer res = ByteBuffer.allocate(4);
        channel = fs.open("test1");
        channel.read(res);
        assertEquals(1, res.get(3));

        channel.close();
        channel = fs.append("test1");
        channel.write(ByteBuffer.wrap(new byte[]{67}));
        channel.close();

        res = ByteBuffer.allocate(5);
        channel = fs.open("test1");
        channel.read(res);
        assertEquals(67, res.get(4));
    }

    public void testDiskFileSystem() throws Exception {
        DiskFileSystem fs = new DiskFileSystem();

        DatastoreChannel channel = fs.create("test1");

        assertTrue(fs.exists("test1"));
        assertFalse(fs.exists("foo"));

        channel.write(ByteBuffer.wrap(new byte[]{0, 0, 0, 1}));
        channel.close();

        ByteBuffer res = ByteBuffer.allocate(4);
        channel = fs.open("test1");
        channel.read(res);
        assertEquals(1, res.get(3));

        channel.close();
        channel = fs.append("test1");
        channel.write(ByteBuffer.wrap(new byte[]{67}));
        channel.close();

        res = ByteBuffer.allocate(5);
        channel = fs.open("test1");
        channel.read(res);
        assertEquals(67, res.get(4));

        assertTrue(fs.exists("test1"));
        fs.remove("test1");
        assertFalse(fs.exists("test1"));

    }

}
