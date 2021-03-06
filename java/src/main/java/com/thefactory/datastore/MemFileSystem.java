package com.thefactory.datastore;

import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.lang.Override;

public class MemFileSystem implements FileSystem {

    private final HashMap<String, Lock> locks = new HashMap<String, Lock>();
    private final HashMap<String, ChannelBuffer> buffers = new HashMap<String, ChannelBuffer>();
    private final HashMap<String, Collection<String>> lists = new HashMap<String, Collection<String>>();

    @Override
    public DatastoreChannel create(String name) {
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        buffers.put(name, buffer);
        return new MemFileSystemChannel(buffer);
    }

    @Override
    public DatastoreChannel open(String name) {
        ChannelBuffer buffer = buffers.get(name);
        if(buffer == null) {
            throw new IllegalArgumentException("Not found: " + name);
        }
        buffer.readerIndex(0);
        return new MemFileSystemChannel(buffer);
    }

    @Override
    public DatastoreChannel append(String name) {
        ChannelBuffer buffer = buffers.get(name);
        if(buffer == null) {
            throw new IllegalArgumentException("Not found: " + name);
        }
        return new MemFileSystemChannel(buffer);
    }

    @Override
    public boolean exists(String name) {
        return (buffers.containsKey(name) || locks.containsKey(name) || lists.containsKey(name));
    }

    @Override
    public void remove(String name) {
        buffers.remove(name);
    }

    @Override
    public void rename(String oldName, String newName) {
        ChannelBuffer buffer = buffers.get(oldName);
        if(buffer == null) {
            throw new IllegalArgumentException("Not found: " + oldName);
        }
        buffers.put(newName, buffer);
    }

    @Override
    public void mkdirs(String path) {
    }

    @Override
    public Closeable lock(String name) throws IOException{
        synchronized (locks) {
            if(locks.containsKey(name)) {
                throw new IOException("Resource is already locked: " + name);
            }
            Lock lock = new Lock(name, locks);
            locks.put(name, lock);
            return lock;
        }
    }

    @Override
    public void storeList(Collection<String> items, String name){
        lists.put(name, items);
    }

    @Override
    public Collection<String> loadList(String name){
        Collection<String> ret = lists.get(name);
        if(ret == null) {
            throw new IllegalArgumentException("List not found: " + name);
        }
        return ret;
    }

    @Override
    public long size(String name) {
        ChannelBuffer buffer = buffers.get(name);
        if(buffer == null) {
            throw new IllegalArgumentException("Not found: " + name);
        }
        return buffer.readableBytes();
    }

    private class Lock implements Closeable {
        private Map<String, Lock> locks;
        private String name;

        private Lock(String name, Map<String, Lock> locks){
            this.name = name;
            this.locks = locks;
        }

        public void close() {
            synchronized(locks){
                locks.remove(name);
            }
        }
    }

    private class MemFileSystemChannel implements DatastoreChannel{

        private final ChannelBuffer buffer;

        private MemFileSystemChannel(ChannelBuffer buffer){
            this.buffer = buffer;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            buffer.readBytes(dst);
            return dst.capacity();
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException{
            buffer.getBytes((int)position, dst);
            return dst.capacity();
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            buffer.writeBytes(src);
            return src.capacity();
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public long size() throws IOException {
            return buffer.writerIndex();
        }

        @Override
        public final boolean isOpen() {
            return true;
        }
    }
}