package com.thefactory.datastore;

import junit.framework.TestCase;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Random;

public class TransactionLogTests extends TestCase {
    private FileSystem fs = null;
    private Random random = new Random(965);

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        fs = new MemFileSystem();
    }

    @Override
    protected void tearDown() throws Exception {
        fs = null;
    }

    private void createFile(String name, byte[] data) throws IOException {
        DatastoreChannel channel = fs.create(name);
        channel.write(ByteBuffer.wrap(data));
    }

    public void testTransactionLogReaderSingle() throws Exception {
        byte[] bytes = new byte[]{ (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,    
                                   (byte)0x01,                                          
                                   (byte)0x00, (byte)0x0A,   
                                   (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, 
                                   (byte)0x29, (byte)0xD1, (byte)0x04, (byte)0x58, 
                                   (byte)0x67, (byte)0xC1 };

        createFile("test.log", bytes);
        TransactionLog.Reader reader = new TransactionLog(fs).getReader("test.log");

        Iterator<Slice> iterator = reader.transactions();
        assertTrue(iterator.hasNext());
        Slice transaction = iterator.next();

        assertEquals(new Slice(bytes).subslice(7), transaction);
    }

    public void testTransactionLogReaderSingleFirstLast() throws Exception {
        byte[] bytes = new byte[] { (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,    // checksum.
                                    (byte)0x02,                                        // type (First).
                                    (byte)0x00, (byte)0x0A,                            // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, 
                                    (byte)0x84, (byte)0x29, (byte)0xD1, 
                                    (byte)0x04, (byte)0x58, (byte)0x67, 
                                    (byte)0xC1,
                                    (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,    // checksum.
                                    (byte)0x04,                                        // type (Last).
                                    (byte)0x00, (byte)0x0A,                            // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, 
                                    (byte)0x29, (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1 };

        createFile("test.log", bytes);
        TransactionLog.Reader reader = new TransactionLog(fs).getReader("test.log");

        Slice orig = new Slice(bytes);
        Iterator<Slice> iterator = reader.transactions();
        assertTrue(iterator.hasNext());
        Slice transaction = iterator.next();

        assertEquals(transaction.subslice(0, 10), orig.subslice(TransactionLog.HEADER_SIZE, 10));
        assertEquals(transaction.subslice(10, 10), orig.subslice(2*TransactionLog.HEADER_SIZE + 10, 10));
    }

    public void testTransactionLogReaderSingleFirstMiddleLast() throws Exception {
        byte[] bytes = new byte[] { (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,             // checksum.
                                    (byte)0x02,                                                 // type (First).
                                    (byte)0x00, (byte)0x0A,                                     // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1,
                                    (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,             // checksum.
                                    (byte)0x03,                                                 // type (Middle).
                                    (byte)0x00, (byte)0x0A,                                     // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1,
                                    (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,             // checksum.
                                    (byte)0x04,                                                 // type (Last).
                                    (byte)0x00, (byte)0x0A,                                     // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1 };

        createFile("test.log", bytes);
        TransactionLog.Reader reader = new TransactionLog(fs).getReader("test.log");

        Slice orig = new Slice(bytes);
        Iterator<Slice> iterator = reader.transactions();
        assertTrue(iterator.hasNext());
        Slice transaction = iterator.next();

        assertEquals(transaction.subslice(0, 10), orig.subslice(TransactionLog.HEADER_SIZE, 10));
        assertEquals(transaction.subslice(10, 10), orig.subslice(2 * TransactionLog.HEADER_SIZE + 10, 10));
        assertEquals(transaction.subslice(20, 10), orig.subslice(3 * TransactionLog.HEADER_SIZE + 20, 10));
    }

    public void testTransactionLogReaderSingleBadRecordType() throws Exception {
        byte[] bytes = new byte[] { (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,             // checksum.
                                    (byte)0x03,                                                 // type (Middle).
                                    (byte)0x00, (byte)0x0A,                                     // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1 };
        createFile("test.log", bytes);
        TransactionLog.Reader reader = new TransactionLog(fs).getReader("test.log");
        Iterator<Slice> iterator = reader.transactions();
        while(iterator.hasNext()){
            try{
                Slice transaction = iterator.next();
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
    }

    public void testTransactionLogReaderSingleBadSecondRecordType() throws Exception {
        byte[] bytes = new byte[] { (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,             // checksum.
                                    (byte)0x02,                                                 // type (First).
                                    (byte)0x00, (byte)0x0A,                                     // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1,
                                    (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,             // checksum.
                                    (byte)0x01,                                                 // type (Full).
                                    (byte)0x00, (byte)0x0A,                                     // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1 };

        createFile("test.log", bytes);
        TransactionLog.Reader reader = new TransactionLog(fs).getReader("test.log");
        Iterator<Slice> iterator = reader.transactions();
        while(iterator.hasNext()){
            try{
                Slice transaction = iterator.next();
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
    }

    public void testTransactionLogReaderSingleBadChecksum() throws Exception {
        byte[] bytes = new byte[] { (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0x00,    // checksum (bad).
                                    (byte)0x01,                                        // type (Full).
                                    (byte)0x00, (byte)0x0A,                            // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1 };

        createFile("test.log", bytes);
        TransactionLog.Reader reader = new TransactionLog(fs).getReader("test.log");
        Iterator<Slice> iterator = reader.transactions();
        while(iterator.hasNext()){
            try{
                Slice transaction = iterator.next();
                assertTrue(false);
            } catch (NumberFormatException e) {
                assertTrue(true);
            }
        }
    }

    public void testTransactionLogReaderReplaySingle() throws Exception {
        byte[] bytes = new byte[] { (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,    // checksum.
                                    (byte)0x01,                                        // type (Full).
                                    (byte)0x00, (byte)0x0A,                            // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1  };

        createFile("test.log", bytes);
        TransactionLog.Reader reader = new TransactionLog(fs).getReader("test.log");
        Iterator<Slice> iterator = reader.transactions();
        int count = 0;
        while(iterator.hasNext()){
            Slice transaction = iterator.next();
            count += 1;
        }
        assertEquals(count, 1);
    }

    public void testTransactionLogReaderReplayFirstBad() throws Exception {
        byte[] bytes = new byte[] { (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,             // checksum.
                                    (byte)0x04,                                                 // type (Last).
                                    (byte)0x00, (byte)0x0A,                                     // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1,
                                    (byte)0xB2, (byte)0x16, (byte)0x3A, (byte)0xFF,             // checksum.
                                    (byte)0x01,                                                 // type (Full).
                                    (byte)0x00, (byte)0x0A,                                     // length.
                                    (byte)0x82, (byte)0xD1, (byte)0xAF, (byte)0x84, (byte)0x29, 
                                    (byte)0xD1, (byte)0x04, (byte)0x58, (byte)0x67, (byte)0xC1};

        createFile("test.log", bytes);
        TransactionLog.Reader reader = new TransactionLog(fs).getReader("test.log");
        Iterator<Slice> iterator = reader.transactions();
        int count = 0;
        while(iterator.hasNext()){
            try{
                Slice transaction = iterator.next();
                count += 1;
            } catch (IllegalArgumentException e) {
                assertEquals(count, 0);
            }
        }
    }

    public void testSmallLog() throws Exception {
        assertEquals(10, testTransactionLogWriterReaderRandom(10, 50, "testlog-small"));
    }

    public void testLargeLog() throws Exception {
        testTransactionLogWriterReaderRandom(100000, 50, "testlog-large");        
    }

    public void testManyLogs() throws Exception {
        for(int i = 0; i < 10000; i++){
            testTransactionLogWriterReaderRandom(100, 50, "testlog-many");
        }
    }

    public void testLargeLogLargeTransactions() throws Exception {
        testTransactionLogWriterReaderRandom(10000, 100000, "testlog-xxl");                
    }

    private long testTransactionLogWriterReaderRandom(int txLogSize, int maxTxSize, String logName) throws Exception {
        byte[] bytes = new byte[maxTxSize];
        random.nextBytes(bytes);
        TransactionLog.Writer writer = new TransactionLog(fs).getWriter(logName);
        for(int i = 0; i < txLogSize; i++) {
            int len = random.nextInt(maxTxSize);
            writer.writeTransaction(new Slice(bytes, maxTxSize - len, len));
        }

        TransactionLog.Reader reader = new TransactionLog(fs).getReader(logName);
        Iterator<Slice> iterator = reader.transactions();
        int count = 0;
        while(iterator.hasNext()){
            Slice transaction = iterator.next();
            count += 1;
        }
        fs.remove(logName);
        return count;
    }

}