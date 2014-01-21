package com.thefactory.datastore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileManager {
    public final String dir;

    public static final String LOCK_FILE = "access.lock";
    public static final String TABLET_WRITE_LOG_FILE = "write.log";
    public static final String TABLET_META_FILE = "stack.txt";

    public FileManager(String dir) {
        this.dir = dir;
    }

    public String dbFilename(String filename) {
        return new File(dir, filename).getAbsolutePath();
    }

    public String getLockFile() {
        return dbFilename(LOCK_FILE);
    }

    public String getTransactionLog() {
        return dbFilename(TABLET_WRITE_LOG_FILE);
    }

    public String getTabletMetaFile() {
        return dbFilename(TABLET_META_FILE);
    }

    public List<String> getTabletFilenames() throws IOException {
        List<String> ret = new ArrayList<String>();
        FileReader fr = new FileReader(getTabletMetaFile());
        try {
            BufferedReader reader = new BufferedReader(fr);
            String line;
            while((line = reader.readLine()) != null){
                ret.add(line.trim());
            }
            return ret;
        } finally {
            fr.close();
        }
    }

    public void writeTabletFilenames(List<String> filenames) throws IOException {
        FileWriter fw = new FileWriter(getTabletMetaFile());
        try {
            for(String filename: filenames) {
                fw.write(String.format("%s\n", filename));
            }
        } finally {
            fw.close();
        }
    }
}

