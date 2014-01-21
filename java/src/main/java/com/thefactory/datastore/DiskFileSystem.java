package com.thefactory.datastore;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.lang.Override;


public class DiskFileSystem implements FileSystem {

    @Override
    public DatastoreChannel create(String name) {
        try {
            return new FileDatastoreChannel(new FileOutputStream(new File(name)).getChannel());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create " + name + ": " + e);
        }
    }

    @Override
    public DatastoreChannel open(String name) {
        try {
            return new FileDatastoreChannel(new FileInputStream(new File(name)).getChannel());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to open " + name + " for writing: " + e);
        }
    }

    @Override
    public DatastoreChannel append(String name) {
        try {
            return new FileDatastoreChannel(new FileOutputStream(new File(name), true).getChannel());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to open " + name + " for appending: " + e);
        }
    }

    @Override
    public boolean exists(String name) {
        return new File(name).exists();
    }

    @Override
    public void remove(String name) {
        if(!new File(name).delete()) {
            throw new IllegalArgumentException("Failed to delete file: " + name);
        }
    }

    @Override
    public void rename(String oldName, String newName) {
        if(!new File(oldName).renameTo(new File(newName))) {
            throw new IllegalArgumentException("Failed to rename file: " + oldName + " to " + newName);
        }
    }

    @Override
    public void mkdirs(String path) {
        if(!new File(path).mkdirs()) {
            throw new IllegalArgumentException("Failed to create path: " + path);
        }
    }

    @Override
    public Closeable lock(String name) throws IOException{
        FileChannel fc = new FileInputStream(new File(name)).getChannel();
        FileLock lock = fc.tryLock();
        if(lock == null){
            throw new IOException("Failed to obtain lock for " + name);
        }
        return new Lock(lock);
    }

    @Override
    public String[] list(String dir) {
        File d = new File(dir);
        if(!d.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + dir);
        }
        return d.list();
    }

    private class FileDatastoreChannel implements DatastoreChannel{
        
        private final FileChannel channel;
        
        private FileDatastoreChannel(FileChannel channel){
            this.channel = channel;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return channel.read(dst);
        }

        @Override
        public long read(ByteBuffer dst, long position) throws IOException{
            return channel.read(dst, position);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return channel.write(src);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        @Override
        public final boolean isOpen() {
            return channel.isOpen();
        }
    }

    private class Lock implements Closeable {

        private final FileLock lock;

        private Lock(FileLock lock){
            this.lock = lock;
        }

        @Override
        public void close() throws IOException {
            lock.release();
        }
    }


}