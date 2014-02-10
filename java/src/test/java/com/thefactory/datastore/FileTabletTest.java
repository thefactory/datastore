package com.thefactory.datastore;

import junit.framework.TestCase;
import java.util.Iterator;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.util.List;
import org.xerial.snappy.Snappy;

import static org.junit.Assert.assertArrayEquals;

public class FileTabletTest extends TestCase {

    public void testTabletFindWithTermSimple() throws IOException {
        // small test tablet defined inline
        byte[] bytes  = new byte[] {
            0,                                                // H: checksum.
            0,                                                // H: type (uncompressed).
            17,                                               // H: length.
            0,                                                // 0-byte key prefix.
            (byte)0xa3, 1, 2, 3,                              // 3-byte key suffix.
            (byte)0xa3, 4, 5, 6,                              // 3-byte value.
            0, 0, 0, 0,                                       // restart at 0.
            0, 0, 0, 1,                                       // one restart indexes.
            (byte)0x0e, (byte)0xa7, (byte)0xda, (byte)0x7a,   // MetaIndex magic (0x0ea7da7a).
            (byte)0xda, (byte)0x7a, (byte)0xba, (byte)0x5e,   // DataIndex magic (0xda7aba5e).
            0, 20, (byte)0xa3, 1, 2, 3,                       // offset: 0, length: 17, data: 1, 2, 3.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 20,              // MetaIndexOffset msgpack uint64.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 4,               // MetaIndexLength msgpack uint64.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 24,              // DataIndexOffset msgpack uint64.
            (byte)0xcf, 0, 0, 0, 0, 0, 0, 0, 10,              // DataIndexLength msgpack uint64.
            (byte)0x0b, (byte)0x50, (byte)0x1e, (byte)0x7e    // Tablet magic (0x0b501e7e).
            };

        File tmpFile = File.createTempFile("test-tablet", null);    
        FileOutputStream output = new FileOutputStream(tmpFile);
        output.write(bytes);
        output.flush(); 
        output.close();   

        FileTablet tablet = new FileTablet(getFileChannel(tmpFile.getPath()), new TabletReaderOptions());
        Slice term = new Slice(new byte[] {1, 2, 3});
        Iterator<KV> kvs = tablet.find(term);
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), term.toString());
            assertEquals(kv.getValue().toString(), new Slice(new byte[] {4, 5, 6}).toString());
            break;
        }
    }

    public void testTabletFileUncompressed1BlockAll() throws Exception {
        FileTablet tablet = new FileTablet(getFileChannel("test-data/ngrams1/ngrams1-1block-uncompressed.tab"), new TabletReaderOptions());
        Iterator<KV> p = tablet.find();
        BufferedReader reader = new BufferedReader(new FileReader(getFile("test-data/ngrams1/ngrams1.txt")));
        String line;
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes("UTF-8");
            byte[] v = kv[1].getBytes("UTF-8");
            assertTrue(p.hasNext());
            KV item = p.next();
            assertEquals(item.getKey().toString(), new Slice(k).toString());
            assertEquals(item.getValue().toString(), new Slice(v).toString());
        }
    }

    public void testTabletFileUncompressed1BlockFrom1() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(getFile("test-data/ngrams1/ngrams1.txt")));
        String[] kv = reader.readLine().split(" ");
        FileTablet tablet = new FileTablet(getFileChannel("test-data/ngrams1/ngrams1-1block-uncompressed.tab"), new TabletReaderOptions());
        Iterator<KV> p = tablet.find(new Slice(kv[0].getBytes("UTF-8")));
        while(true){
            byte[] k = kv[0].getBytes("UTF-8");
            byte[] v = kv[1].getBytes("UTF-8");
            assertTrue(p.hasNext());
            KV item = p.next();
            assertEquals(item.getKey().toString(), new Slice(k).toString());
            assertEquals(item.getValue().toString(), new Slice(v).toString());
            String line = reader.readLine();
            if(line == null){
                break;
            }
            kv = line.split(" ");
        }
        assertEquals(reader.readLine(), null);
    }

    public void testTabletFileUncompressed1BlockFromN() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(getFile("test-data/ngrams1/ngrams1.txt")));
        // Forward to the 10th entry ...
        String[] kv = {"", ""};
        for(int i = 0; i < 10; i++){
            kv = reader.readLine().split(" ");
        }
        FileTablet tablet = new FileTablet(getFileChannel("test-data/ngrams1/ngrams1-1block-uncompressed.tab"), new TabletReaderOptions());
        Iterator<KV> p = tablet.find(new Slice(kv[0].getBytes("UTF-8")));
        while(true){
            byte[] k = kv[0].getBytes("UTF-8");
            byte[] v = kv[1].getBytes("UTF-8");
            assertTrue(p.hasNext());
            KV item = p.next();
            assertEquals(item.getKey().toString(), new Slice(k).toString());
            assertEquals(item.getValue().toString(), new Slice(v).toString());
            String line = reader.readLine();
            if(line == null){
                break;
            }
            kv = line.split(" ");
        }
        assertEquals(reader.readLine(), null);
    }

    public void testTabletFileCompressed1BlockAll() throws Exception {
        FileTablet tablet = new FileTablet(getFileChannel("test-data/ngrams1/ngrams1-1block-compressed.tab"), new TabletReaderOptions());
        Iterator<KV> p = tablet.find();
        BufferedReader reader = new BufferedReader(new FileReader(getFile("test-data/ngrams1/ngrams1.txt")));
        String line;
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes("UTF-8");
            byte[] v = kv[1].getBytes("UTF-8");
            assertTrue(p.hasNext());
            KV item = p.next();
            assertEquals(item.getKey().toString(), new Slice(k).toString());
            assertEquals(item.getValue().toString(), new Slice(v).toString());
        }
    }

    public void testTabletFileCompressedNBlockAll() throws Exception {
        FileTablet tablet = new FileTablet(getFileChannel("test-data/ngrams1/ngrams1-Nblock-compressed.tab"), new TabletReaderOptions());
        Iterator<KV> p = tablet.find();
        BufferedReader reader = new BufferedReader(new FileReader(getFile("test-data/ngrams1/ngrams1.txt")));
        String line;
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes("UTF-8");
            byte[] v = kv[1].getBytes("UTF-8");
            assertTrue(p.hasNext());
            KV item = p.next();
            assertEquals(item.getKey().toString(), new Slice(k).toString());
            assertEquals(item.getValue().toString(), new Slice(v).toString());
        }
    }

    public void testTabletFileCompressedNBlockFrom1() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(getFile("test-data/ngrams1/ngrams1.txt")));
        String[] kv = reader.readLine().split(" ");
        FileTablet tablet = new FileTablet(getFileChannel("test-data/ngrams1/ngrams1-Nblock-compressed.tab"), new TabletReaderOptions());
        Iterator<KV> p = tablet.find(new Slice(kv[0].getBytes("UTF-8")));
        while(true){
            byte[] k = kv[0].getBytes("UTF-8");
            byte[] v = kv[1].getBytes("UTF-8");
            assertTrue(p.hasNext());
            KV item = p.next();
            assertEquals(item.getKey().toString(), new Slice(k).toString());
            assertEquals(item.getValue().toString(), new Slice(v).toString());
            String line = reader.readLine();
            if(line == null){
                break;
            }
            kv = line.split(" ");
        }
        assertEquals(reader.readLine(), null);
    }

    public void testTabletFileCompressedNBlockFromN() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(getFile("test-data/ngrams1/ngrams1.txt")));
        // Forward to the 10th entry ...
        String[] kv = {"", ""};
        for(int i = 0; i <= 16; i++){
            kv = reader.readLine().split(" ");
        }

        FileTablet tablet = new FileTablet(getFileChannel("test-data/ngrams1/ngrams1-Nblock-compressed.tab"), new TabletReaderOptions());
        Iterator<KV> p = tablet.find(new Slice(kv[0].getBytes("UTF-8")));
        while(true){
            byte[] k = kv[0].getBytes("UTF-8");
            byte[] v = kv[1].getBytes("UTF-8");
            assertTrue(p.hasNext());
            KV item = p.next();
            assertEquals(item.getKey().toString(), new Slice(k).toString());
            assertEquals(item.getValue().toString(), new Slice(v).toString());
            String line = reader.readLine();
            if(line == null){
                break;
            }
            kv = line.split(" ");
        }
        assertEquals(reader.readLine(), null);
    }

    public void testFull1BlockUncompressed() throws Exception {
        assertTrue(testForAllKeys("test-data/ngrams1/ngrams1-1block-uncompressed.tab", 0));
    }
    
    public void testFull1BlockCompressed() throws Exception {
        assertTrue(testForAllKeys("test-data/ngrams1/ngrams1-1block-compressed.tab", 0));
    }
    
    public void testFullNBlockCompressed() throws Exception {
        assertTrue(testForAllKeys("test-data/ngrams1/ngrams1-Nblock-compressed.tab", 2));
    }

    // Get a test file so that we can run from the java subtree
    // or top level
    private File getFile(String path) throws FileNotFoundException {
        File ret = new File(path);
        if(ret.exists()){
            return ret;
        }
        ret = new File(".." + File.separatorChar + path);
        if(!ret.exists()){
            throw new FileNotFoundException("Test file not found: " + path);
        }
        return ret;
    } 

    private DatastoreChannel getFileChannel(String path) throws FileNotFoundException {
        return new DiskFileSystem().open(getFile(path).getPath());
    }

    private void dumpTablet(File tabletFile) throws Exception {
        FileTablet tablet = new FileTablet(getFileChannel(tabletFile.getPath()), new TabletReaderOptions());
        System.out.println("Index:\n---------------------------------------------------");
        List<TabletReader.TabletIndexRecord> index = tablet.index();
        for(int i = 0; i < index.size(); i++){
            TabletReader.TabletIndexRecord idx = index.get(i);
            System.out.println("ofs: " + idx.offset + " len: " + idx.length + " data: " + idx.data.toUTF8String());
        }

        List<BlockReader> blocks = tablet.blocks();
        for(int i = 0; i < blocks.size(); i++){
            System.out.println("\nBlock " + i + "\n---------------------------------------------------");
            BlockReader block = blocks.get(i);
            int k = 0;
            Slice restart;
            while((restart = block.restartKey(k)) != null) {
                System.out.println(restart.toUTF8String());                
            }
            System.out.println(k + " restarts:");
            System.out.println("\nData:");            
            Iterator<KV> kvs = block.find();
            while(kvs.hasNext()){
                KV kv = kvs.next();
                System.out.println(kv.getKey().toUTF8String() + " => " + kv.getValue().toUTF8String());                
            }

        }
    }

    private boolean testForAllKeys(String fileName, int startKey) throws Exception {
        int nlines = 0;
        BufferedReader r = new BufferedReader(new FileReader(getFile("test-data/ngrams1/ngrams1.txt")));
        // get number of lines in the reference file
        while(r.readLine() != null)
            nlines += 1;

        for(int n = startKey; n < nlines; n++){
            BufferedReader reader = new BufferedReader(new FileReader(getFile("test-data/ngrams1/ngrams1.txt")));
            // Forward to the k th entry ...
            String[] kv = {"", ""};
            for(int i = 0; i <= n; i++){
                kv = reader.readLine().split(" ");
            }

            FileTablet tablet = new FileTablet(getFileChannel("test-data/ngrams1/ngrams1-Nblock-compressed.tab"), new TabletReaderOptions());
            Iterator<KV> p = tablet.find(new Slice(kv[0].getBytes("UTF-8")));
            while(true){
                byte[] k = kv[0].getBytes("UTF-8");
                byte[] v = kv[1].getBytes("UTF-8");
                if(!p.hasNext()){
                    dumpTablet(getFile("test-data/ngrams1/ngrams1-Nblock-compressed.tab"));
                    System.out.println("KV iterator failed at " + kv[0]);
                    return false;
                }
                KV item = p.next();
                if((Slice.compare(item.getKey(), new Slice(k)) != 0) || (Slice.compare(item.getValue(), new Slice(v)) != 0)){
                    dumpTablet(getFile("test-data/ngrams1/ngrams1-Nblock-compressed.tab"));
                    System.out.println("Failed item: k = " + kv[0] + " v = " + kv[1] + "  not equals " + item.getKey().toUTF8String());
                    return false;
                }
                String line = reader.readLine();
                if(line == null){
                    break;
                }
                kv = line.split(" ");
            }
            if(reader.readLine() != null){
                dumpTablet(getFile("test-data/ngrams1/ngrams1-Nblock-compressed.tab"));
                System.out.println("Incorrect key count (missing keys in tablet file)");
                return false;
            }
        }

        return true;
    }
}