package com.thefactory.datastore;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

public class Utils {

    public static long toUInt32(Slice slice) {
        return (long)(slice.getAt(0) << 24 | slice.getAt(1) << 16 | slice.getAt(2) << 8 | slice.getAt(3)) & 0xffffffff;
    }

    public static int commonPrefix(byte[] bin1, byte[] bin2) {
        int num = Math.min(bin1.length, bin2.length);
        int count = 0;

        for (int i=0; i<num; i++) {
            if (bin1[i] == bin2[i]) {
                count++;
            } else {
                break;
            }
        }

        return count;
    }

    public static boolean deletePathRecursive(File path) {
        if (path.isDirectory()) {
            for (File file : path.listFiles()) {
                if (!deletePathRecursive(file))
                    return false;
            } 
        }
        return path.delete();
    }   

    public static File createTempDirectory(String prefix) throws IOException {
        File temp = File.createTempFile(prefix, null);
        if(!temp.delete()) {
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }
        if(!(temp.mkdir())) {
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }
        return (temp);
    }

    public static void fileCopy(final File dest, final File src) throws IOException {
        FileInputStream in = null;
        FileOutputStream out = null;
        try {
            in = new FileInputStream(src);
            out = new FileOutputStream(dest);
            FileChannel fcin =  in.getChannel();
            FileChannel fcout =  out.getChannel();
            long bytes =  fcin.size();
            while(bytes > 0L) {
                bytes -= fcout.transferFrom(fcin, 0, bytes);
            }
        } finally {
            if(in != null) {
                in.close();
            }
            if(out != null) {
                out.close();
            }
        }
    }
}
