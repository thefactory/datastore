package com.thefactory.datastore;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileManager {
    public final String dir;

    public static final String LOCK_FILE = "access.lock";
    public static final String TABLET_WRITE_LOG_FILE = "write.log";
    public static final String IMMUTABLE_TABLET_WRITE_LOG_FILE = "write_imm.log";
    public static final String TABLET_META_FILE = "stack.txt";

    private final FileSystem fs;

    public FileManager(String dir, FileSystem fs, boolean createIfMissing) {
        this.dir = dir;
        this.fs = fs;
        if(!fs.exists(dir)) {
            if(!createIfMissing) {
                throw new IllegalArgumentException("database path not found: " + dir);
            }
            fs.mkdirs(dir);
        }
    }

    public String dbFilename(String filename) {
        return new File(dir, filename).getPath();
    }

    public String getLockFile() {
        return dbFilename(LOCK_FILE);
    }

    public String getTransactionLog() {
        return dbFilename(TABLET_WRITE_LOG_FILE);
    }

    public String getSecondaryTransactionLog() {
        return dbFilename(IMMUTABLE_TABLET_WRITE_LOG_FILE);
    }

    public String getTabletMetaFile() {
        return dbFilename(TABLET_META_FILE);
    }

    public Collection<String> loadTabletFilenames() throws IOException {
        if (fs.exists(dbFilename(TABLET_META_FILE))) {
            return fs.loadList(getTabletMetaFile());
        }

        return new ArrayList<String>();
    }

    public void writeTabletFilenames(Collection<String> filenames) throws IOException {
        fs.storeList(filenames, getTabletMetaFile());
    }

    public boolean exists(String filename, int maxAttempts) {
        int attempts = maxAttempts;
        while(attempts > 0) {
            if(fs.exists(dbFilename(filename))) {
                return true;
            }
            try{
                java.lang.Thread.sleep(100 * attempts);
            } catch (InterruptedException e) {
                ;
            }
            attempts --;
        }

        return false;
    }
}

