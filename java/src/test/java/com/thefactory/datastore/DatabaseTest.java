package com.thefactory.datastore;

import junit.framework.TestCase;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Random;
import java.util.Date;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.Override;
import org.junit.*;

public class DatabaseTest extends TestCase {

    private static String DB_PATH = "db";
    private static int MAX_SLICE_SIZE = 1024 * 1024;
    private static Random random = new Random(965);
    private static byte[] randomBytes = new byte[MAX_SLICE_SIZE];

    {
        random.nextBytes(randomBytes);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        rmDBDir();
    }

    public void testCreateIfMissingMemFileSystem() throws Exception {
        FileSystem fs = new MemFileSystem();
        Database db = Database.open(DB_PATH, new Database.Options(fs));
        assertTrue(fs.exists(new File(DB_PATH, FileManager.LOCK_FILE).getPath())); 
        assertTrue(fs.exists(new File(DB_PATH, FileManager.TABLET_WRITE_LOG_FILE).getPath())); 
        assertFalse(fs.exists(new File(DB_PATH, FileManager.TABLET_META_FILE).getPath())); 

        try {
            db = Database.open("db", new Database.Options(new MemFileSystem(), false, false, 1024 * 1024 * 4));
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    public void testCreateIfMissingDiskFileSystem() throws Exception {
        FileSystem fs = new DiskFileSystem();
        Database db = Database.open(DB_PATH, new Database.Options(fs));
        assertTrue(fs.exists(new File(DB_PATH, FileManager.LOCK_FILE).getPath()));       
        assertTrue(fs.exists(new File(DB_PATH, FileManager.TABLET_WRITE_LOG_FILE).getPath()));         
        assertFalse(fs.exists(new File(DB_PATH, FileManager.TABLET_META_FILE).getPath())); 
        
        rmDBDir();
        try {
            db = Database.open("db", new Database.Options(new DiskFileSystem(), false, false, 1024 * 1024 * 4));
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
       
    }

    public void testDatabaseLockMemFileSystem() throws Exception {
        FileSystem fs = new MemFileSystem();
        Database one = Database.open(DB_PATH, new Database.Options(fs));

        try{
            Database two = Database.open(DB_PATH, new Database.Options(fs));
            assertTrue(false);
        } catch (IOException e) {
            assertTrue(true);
        }
    }

    public void testDatabaseLockDiskFileSystem() throws Exception {
        FileSystem fs = new DiskFileSystem();
        Database one = Database.open(DB_PATH, new Database.Options(fs));

        try{
            Database two = Database.open(DB_PATH, new Database.Options(fs));
            assertTrue(false);
        } catch (IOException e) {
            assertTrue(true);
        }
    }

    public void testPutFindGet() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), new String[]{});

        Iterator<KV> kvs = db.find();
        assertFalse(kvs.hasNext());

        Slice key = nextRandomSlice(100);
        Slice value = nextRandomSlice(100);

        db.put(key, value);

        kvs = db.find(key);
        assertTrue(kvs.hasNext());
        KV kv = kvs.next();
        assertEquals(0, Slice.compare(kv.getKey(), key));
        assertEquals(0, Slice.compare(kv.getValue(), value));
        assertFalse(kvs.hasNext());

        Slice res = db.get(key);
        assertEquals(0, Slice.compare(res, value));
    }

    public void testMultiplePut() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), new String[]{});

        Iterator<KV> kvs = db.find();
        assertFalse(kvs.hasNext());

        Slice key = nextRandomSlice(100);
        Slice value = nextRandomSlice(100);

        db.put(key, nextRandomSlice(100));
        db.put(key, value);
        assertEquals(db.get(key), value);
    }

    public void testDeleteWithFind() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), new String[]{});

        Iterator<KV> kvs = db.find();
        assertFalse(kvs.hasNext());

        Slice key = nextRandomSlice(100);
        Slice value = nextRandomSlice(100);

        db.put(key, value);

        kvs = db.find(key);
        assertTrue(kvs.hasNext());
        KV kv = kvs.next();
        assertEquals(0, Slice.compare(kv.getKey(), key));
        assertEquals(0, Slice.compare(kv.getValue(), value));
        assertFalse(kvs.hasNext());

        db.delete(key);
        kvs = db.find(key);
        assertFalse(kvs.hasNext());
    }

    public void testDeleteWithGet() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), new String[]{});

        Iterator<KV> kvs = db.find();
        assertFalse(kvs.hasNext());

        Slice key = nextRandomSlice(100);
        Slice value = nextRandomSlice(100);

        db.put(key, value);
        assertEquals(0, Slice.compare(db.get(key), value));
        db.delete(key);

        try {
            db.get(key);
            assertTrue(false);
        } catch (KeyNotFoundException e) {
            assertTrue(true);
            return;
        }
        assertTrue(false);
    }

    public void testManyKeysWithMultipleTablets() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), new String[]{});

        Iterator<KV> kvs = db.find();
        assertFalse(kvs.hasNext());

        Slice key = new Slice("A special key that is not random".getBytes());
        Slice value = new Slice("A special value for our key".getBytes());
        db.put(key, value);
        // Add many keys (+4MBytes) to ensure that we trigger flushing our memory tablet to disk ...
        for(int i = 0; i < 10000; i++){
            db.put(nextRandomSlice(1000), nextRandomSlice(1000));
        }

        assertEquals(db.get(key), value);
    }

    public void testPutOneGet2WithPrefixedKeys() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), new String[]{});
        assertFalse(db.find().hasNext());
    
        Slice key = new Slice("A special key that is not random".getBytes());
        Slice value = new Slice("A special value for our key".getBytes());

        db.put(key, value);
        // Add many keys (+4MBytes) to ensure that we trigger flushing our memory tablets to disk ...
        for(int i = 0; i < 10000; i++){
            Slice krand = nextPrefixedRandomSlice(1000);
            Slice vrand = nextRandomSlice(10000);
            db.put(krand, vrand);
            assertEquals(db.get(key), value);
            assertEquals(db.get(krand), vrand);
        }
        assertEquals(db.get(key), value);
    }

    public void testPutOneGet2WithRandomKeys() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), new String[]{});
        assertFalse(db.find().hasNext());
    
        Slice key = new Slice("A special key that is not random".getBytes());
        Slice value = new Slice("A special value for our key".getBytes());

        db.put(key, value);
        // Add many keys (+4MBytes) to ensure that we trigger flushing our memory tablets to disk ...
        for(int i = 0; i < 10000; i++){
            Slice krand = nextRandomSlice(1000);
            Slice vrand = nextRandomSlice(10000);
            db.put(krand, vrand);
            assertEquals(db.get(key), value);
            assertEquals(db.get(krand), vrand);
        }
        assertEquals(db.get(key), value);
    }

    public void testDatabaseOneFileTabletFindAll() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams1/ngrams1-Nblock-compressed.tab"
                                        });
        Iterator<KV> it = db.find();
        BufferedReader reader = new BufferedReader(new FileReader("../test-data/ngrams1/ngrams1.txt"));
        String line;
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            byte[] v = kv[1].getBytes();
            assertTrue(it.hasNext());
            KV item = it.next();
            assertEquals(0, Slice.compare(item.getKey(), new Slice(k)));
            assertEquals(0, Slice.compare(item.getValue(), new Slice(v)));
        }
    }

    public void testDatabaseMultiFileTabletFindAll() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams2/ngrams.tab.0" ,
                                            "../../test-data/ngrams2/ngrams.tab.1"
                                        });
        Iterator<KV> it = db.find();      
        BufferedReader reader = new BufferedReader(new FileReader("../test-data/ngrams2/ngrams2.txt"));
        String line;
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            byte[] v = kv[1].getBytes();
            assertTrue(it.hasNext());
            KV item = it.next();
            assertEquals(0, Slice.compare(item.getKey(), new Slice(k)));
            assertEquals(0, Slice.compare(item.getValue(), new Slice(v)));
        }
       
    }

    public void testDatabaseMultiFileTabletFindFromN() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams2/ngrams.tab.0" ,
                                            "../../test-data/ngrams2/ngrams.tab.1"
                                        });
        BufferedReader reader = new BufferedReader(new FileReader("../test-data/ngrams2/ngrams2.txt"));
        String line = "";
        int n = 0;
        while((n < 10) && ((line = reader.readLine()) != null)){
            n += 1;
        }
        Slice term = new Slice(line.split(" ")[0].getBytes());
        Iterator<KV> it = db.find(term);      
        do {
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            byte[] v = kv[1].getBytes();
            assertTrue(it.hasNext());
            KV item = it.next();
            assertEquals(0, Slice.compare(item.getKey(), new Slice(k)));
            assertEquals(0, Slice.compare(item.getValue(), new Slice(v)));
        } while((line = reader.readLine()) != null);
    }

    public void testDatabaseMultiFileTabletGetHit() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams2/ngrams.tab.0" ,
                                            "../../test-data/ngrams2/ngrams.tab.1"
                                        });
        Iterator<KV> it = db.find();      
        BufferedReader reader = new BufferedReader(new FileReader("../test-data/ngrams2/ngrams2.txt"));
        String line = reader.readLine();
        String[] kv = line.split(" ");
        byte[] k = kv[0].getBytes();
        byte[] v = kv[1].getBytes();
        assertEquals(0, Slice.compare(db.get(new Slice(k)), new Slice(v)));
    }

    public void testDatabaseMultiFileTabletGetMiss() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams2/ngrams.tab.0" ,
                                            "../../test-data/ngrams2/ngrams.tab.1"
                                        });
        try {
            Slice v = db.get(new Slice("Key which does not exist".getBytes()));  
            assertTrue(false);
        } catch (KeyNotFoundException e) {
            assertTrue(true);
            return;
        } 
        assertTrue(false);
    }

    public void testDatabaseOverwriteAll() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams1/ngrams1-Nblock-compressed.tab"
                                        });
        BufferedReader reader = new BufferedReader(new FileReader("../test-data/ngrams1/ngrams1.txt"));
        String line;
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            db.put(new Slice(k), new Slice("Overwritten".getBytes()));
        }

        reader = new BufferedReader(new FileReader("../test-data/ngrams1/ngrams1.txt"));
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            assertEquals(db.get(new Slice(k)), new Slice("Overwritten".getBytes()));
        }
    }

    public void testDatabaseOverwriteAllMultipelTablets() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams1/ngrams1-Nblock-compressed.tab",
                                            "../../test-data/ngrams2/ngrams.tab.1"
                                        });

        BufferedReader reader = new BufferedReader(new FileReader("../test-data/ngrams2/ngrams2.txt"));
        String line;
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            db.put(new Slice(k), new Slice("Overwritten".getBytes()));
        }

        reader = new BufferedReader(new FileReader("../test-data/ngrams2/ngrams2.txt"));
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            assertEquals(db.get(new Slice(k)), new Slice("Overwritten".getBytes()));
        }
    }

    public void testDatabaseOverwriteFromN() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams1/ngrams1-Nblock-compressed.tab"
                                        });
        int n = 10;
        BufferedReader reader = new BufferedReader(new FileReader("../test-data/ngrams1/ngrams1.txt"));
        String line;
        int count = 0;
        while((line = reader.readLine()) != null){
            if(count >= n) {
                String[] kv = line.split(" ");
                byte[] k = kv[0].getBytes();
                db.put(new Slice(k), new Slice("Overwritten".getBytes()));
            }
            count += 1;
        }

        reader = new BufferedReader(new FileReader("../test-data/ngrams1/ngrams1.txt"));
        count = 0;
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            if(count >= n) {
                assertEquals(db.get(new Slice(k)), new Slice("Overwritten".getBytes()));
            } else {
                assertEquals(db.get(new Slice(k)), new Slice( kv[1].getBytes()));
            }
            count += 1;
        }
    }

    public void testDatabaseDeleteAll() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams1/ngrams1-Nblock-compressed.tab"
                                        });
        BufferedReader reader = new BufferedReader(new FileReader("../test-data/ngrams1/ngrams1.txt"));
        String line;
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            db.delete(new Slice(k));
        }

        Iterator<KV> it = db.find();

        while(it.hasNext()){
            System.out.println(it.next().getKey().toUTF8String());
        }
    }

    public void testDatabaseDeleteFromN() throws Exception {
        Database db = setupDatabase(new DiskFileSystem(), 
                                    new String[] 
                                        {
                                            "../../test-data/ngrams1/ngrams1-Nblock-compressed.tab"
                                        });
        int n = 10;
        BufferedReader reader = new BufferedReader(new FileReader("../test-data/ngrams1/ngrams1.txt"));
        String line;
        int count = 0;
        while((line = reader.readLine()) != null){
            if(count >= n) {
                String[] kv = line.split(" ");
                byte[] k = kv[0].getBytes();
                db.delete(new Slice(k));
            }
            count += 1;
        }

        reader = new BufferedReader(new FileReader("../test-data/ngrams1/ngrams1.txt"));
        count = 0;
        Slice other = new Slice("SomeDefaultValue".getBytes());
        while((line = reader.readLine()) != null){
            String[] kv = line.split(" ");
            byte[] k = kv[0].getBytes();
            if(count >= n) {
                assertEquals(Slice.compare(db.getOrElse(new Slice(k), other), other), 0);
            } else {
                assertEquals(db.get(new Slice(k)), new Slice( kv[1].getBytes()));
            }
            count += 1;
        }
    }

    public void testDatabaseReplay() throws Exception {
        FileSystem fs = new DiskFileSystem();
        Database db = setupDatabase(fs, new String[]{});
        for(int i = 0; i < 10; i++) {
            Slice key = new Slice(String.format("key%d", i).getBytes());
            Slice value = new Slice(String.format("value%d", i).getBytes());
            db.put(key, value);
        }
        db.close();

        db = setupDatabase(fs, new String[]{});

        for(int i = 0; i < 10; i++) {
            Slice key = new Slice(String.format("key%d", i).getBytes());
            Slice value = db.get(key);
            assertEquals(Slice.compare(value, new Slice(String.format("value%d", i).getBytes())), 0);
        }
    }

    public void testDatabaseFindByPrefix() throws Exception {
        FileSystem fs = new DiskFileSystem();
        Database db = setupDatabase(fs, new String[]{});
        for(int i = 0; i < 100; i++) {
            Slice key = new Slice(String.format("key%04d", i).getBytes());
            Slice value = new Slice(String.format("value%04d", i).getBytes());
            db.put(key, value);
        }

        int count = 0;
        Iterator<KV> it = db.find(new Slice("key".getBytes()));
        while(it.hasNext()) {
            KV kv = it.next();
            assertEquals(Slice.compare(kv.getKey(), new Slice(String.format("key%04d", count).getBytes())), 0);
            count++;
        }
        assertEquals(count, 100);
    }

    private Slice nextRandomSlice(int maxSize) {
        if(maxSize >= MAX_SLICE_SIZE) {
            throw new IllegalArgumentException("slice size exceeds max size of " + MAX_SLICE_SIZE);
        }

        int len = random.nextInt(maxSize);
        Slice r = new Slice(randomBytes, MAX_SLICE_SIZE - len, len);
        return r;
    }

    private Slice nextPrefixedRandomSlice(int maxSize) {
        if(maxSize >= MAX_SLICE_SIZE) {
            throw new IllegalArgumentException("slice size exceeds max size of " + MAX_SLICE_SIZE);
        }

        int len = random.nextInt(maxSize);
        Slice r = new Slice(randomBytes, 0, len);
        return r;
    }

    private Database setupDatabase(FileSystem fs, String[] tablets) throws IOException {
        Database ret = Database.open(DB_PATH, new Database.Options(fs));
        // Tablet paths must be relative to DB_PATH
        for(String tablet : tablets) {
            ret.pushTablet(tablet);
        }

        return ret;
    }

    private void rmDBDir(){
        File dir = new File(DB_PATH);
        String[] files = dir.list();
        if(files == null) {
            return;
        }
        for(String file : files) {
            new File(DB_PATH, file).delete();
        }
        dir.delete();
    }
}

