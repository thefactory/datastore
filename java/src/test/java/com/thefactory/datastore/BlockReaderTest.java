package com.thefactory.datastore;

import junit.framework.TestCase;
import java.util.Iterator;

import static org.junit.Assert.assertArrayEquals;

public class BlockReaderTest extends TestCase {

    public void testBlockIterateOneNoPrefixes() {
        byte[] bytes = new byte[]{0,                   
                                (byte) 0xa3, 1, 2, 3,  
                                (byte) 0xa3, 4, 5, 6,  
                                (byte) 0, 0, 0, 0};    
        BlockReader block = new BlockReader(new Slice(bytes));
        Iterator<KV> kvs = block.find();
        int count = 0;
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), new Slice(new byte[]{1, 2, 3}).toString());
            assertEquals(kv.getValue().toString(), new Slice(new byte[]{4, 5, 6}).toString());
            count++;
          }
        assertEquals(count, 1);    
    }

    public void testBlockIterateManyNoPrefixes() {
        byte[] bytes = new byte[]{0,                   
                                 (byte) 0xa3, 1, 2, 3, 
                                 (byte) 0xa3, 4, 5, 6, 
                                 0,                    
                                 (byte) 0xa3, 1, 2, 3, 
                                 (byte) 0xa3, 4, 5, 6, 
                                 0, 0, 0, 0};          
        BlockReader block = new BlockReader(new Slice(bytes));
        int count = 0;
        Iterator<KV> kvs = block.find();
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), new Slice(new byte[]{1, 2, 3}).toString());
            assertEquals(kv.getValue().toString(), new Slice(new byte[]{4, 5, 6}).toString());
            count++;
        }
        assertEquals(count, 2);    
    }

    public void testBlockIterateManyWithPrefixes() {
        byte[] bytes = new byte[]{0,                    
                                 (byte) 0xa3, 1, 2, 3,  
                                 (byte) 0xa3, 4, 5, 6,  
                                 1,                     
                                 (byte) 0xa2, 2, 3,     
                                 (byte) 0xa3, 4, 5, 6,  
                                 2,                     
                                 (byte) 0xa1, 3,        
                                 (byte) 0xa3, 4, 5, 6,  
                                  3,                    
                                 (byte) 0xa0,           
                                 (byte) 0xa3, 4, 5, 6,  
                                  0, 0, 0, 0};          
        BlockReader block = new BlockReader(new Slice(bytes));
        int count = 0;
        Iterator<KV> kvs = block.find();
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), new Slice(new byte[]{1, 2, 3}).toString());
            assertEquals(kv.getValue().toString(), new Slice(new byte[]{4, 5, 6}).toString());
            count++;
        }
        assertEquals(count, 4);    
    }

    public void testBlockWithRestarts() {
        byte[] bytes = new byte[]{0,                    
                                 (byte) 0xa3, 1, 2, 3,  
                                 (byte) 0xa3, 4, 5, 6,  
                                 0,                     
                                 (byte) 0xa3, 1, 2, 3,  
                                 (byte) 0xa3, 4, 5, 6,  
                                 0, 0, 0, 0,            
                                 0, 0, 0, 9,            
                                 0, 0, 0, 2};           
        BlockReader block = new BlockReader(new Slice(bytes));
        int count = 0;
        Iterator<KV> kvs = block.find();
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), new Slice(new byte[]{1, 2, 3}).toString());
            assertEquals(kv.getValue().toString(), new Slice(new byte[]{4, 5, 6}).toString());
            count++;
        }
        assertEquals(count, 2);    
    }

    public void testBlockMidStream() {
        byte[] bytes = new byte[]{0,                    
                                 (byte) 0xa3, 1, 2, 3,  
                                 (byte) 0xa3, 4, 5, 6,  
                                 0, 0, 0, 0,            
                                 0,                     
                                 (byte) 0xa3, 1, 2, 3,  
                                 (byte) 0xa3, 4, 5, 6,  
                                 0, 0, 0, 0,            
                                 0,                     
                                 (byte) 0xa3, 1, 2, 3,  
                                 (byte) 0xa3, 4, 5, 6,  
                                 0, 0, 0, 0};           
          BlockReader block = new BlockReader(new Slice(bytes, 13, 13));
          int count = 0;
          Iterator<KV> kvs = block.find();
          while(kvs.hasNext()){
              KV kv = kvs.next();
              assertEquals(kv.getKey().toString(), new Slice(new byte[]{1, 2, 3}).toString());
              assertEquals(kv.getValue().toString(), new Slice(new byte[]{4, 5, 6}).toString());
              count++;
          }
          assertEquals(count, 1);    
    }

    public void testBlockFindOnRestart() {
        byte[] bytes = new byte[]{0,                    
                                 (byte) 0xa3, 1, 2, 3,  
                                 (byte) 0xa3, 4, 5, 6,  
                                 0,                     
                                 (byte) 0xa3, 2, 3, 4,  
                                 (byte) 0xa3, 4, 5, 6,  
                                 0,                     
                                 (byte) 0xa3, 3, 4, 5,  
                                 (byte) 0xa3, 4, 5, 6,  
                                 0, 0, 0, 0,            
                                 0, 0, 0, 9,            
                                 0, 0, 0, 18,           
                                 0, 0, 0, 3};           
        BlockReader block = new BlockReader(new Slice(bytes));
        Slice term =  new Slice(new byte[] {2, 3, 4});
        Iterator<KV> kvs = block.find(term);
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), term.toString());
            assertEquals(kv.getValue().toString(), new Slice(new byte[] {4, 5, 6}).toString());
            break;
        }
    }

    public void testBlockFindOffRestart() {
        byte[] bytes = new byte[] {0,                    
                                  (byte) 0xa3, 1, 2, 3,  
                                  (byte) 0xa3, 4, 5, 6,  
                                   0,                    
                                  (byte) 0xa3, 1, 2, 4,  
                                  (byte) 0xa3, 4, 5, 6,  
                                   0,                    
                                  (byte) 0xa3, 1, 2, 5,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0,                     
                                  (byte) 0xa3, 1, 2, 6,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0, 0, 0, 0,            
                                  0, 0, 0, 18,           
                                  0, 0, 0, 2};           
        BlockReader block = new BlockReader(new Slice(bytes));
        Slice term =  new Slice(new byte[] {1, 2, 4});
        int count = 0;
        Iterator<KV> kvs = block.find(term);
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), term.toString());
            count += 1;
            break;
        }
        assertEquals(count, 1);
    }

    public void testBlockFindNoRestarts() {
        byte[] bytes = new byte[] {0,                   
                                  (byte) 0xa3, 1, 2, 3, 
                                  (byte) 0xa3, 4, 5, 6, 
                                  1,                    
                                  (byte) 0xa2, 2, 4,    
                                  (byte) 0xa3, 4, 5, 6, 
                                  2,                    
                                  (byte) 0xa1, 5,       
                                  (byte) 0xa3, 4, 5, 6, 
                                  2,                    
                                  (byte) 0xa1, 6,       
                                  (byte) 0xa3, 4, 5, 6, 
                                  0, 0, 0, 0};          
        BlockReader block = new BlockReader(new Slice(bytes));
        Slice term =  new Slice(new byte[] {1, 2, 5});
        Iterator<KV> kvs = block.find(term);
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertTrue(Slice.compare(kv.getKey(), term) >= 0);
        }
    }

    public void testBlockFindUnmatchedBefore() {
        byte[] bytes = new byte[] {0,                    
                                  (byte) 0xa3, 1, 2, 3,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0,                     
                                  (byte) 0xa3, 1, 2, 4,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0,                     
                                  (byte) 0xa3, 1, 2, 5,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0,                     
                                  (byte) 0xa3, 1, 2, 6,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0, 0, 0, 0,            
                                  0, 0, 0, 18,           
                                  0, 0, 0, 2};           
        BlockReader block = new BlockReader(new Slice(bytes));
        Slice term =  new Slice(new byte[] {0, 1, 2});
        Iterator<KV> kvs = block.find(term);
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertTrue(Slice.compare(kv.getKey(), term) > 0);
        }
    }

    public void testBlockFindUnmatchedAfter() {
        byte[] bytes = new byte[] {0,            
                                  (byte) 0xa3, 1, 2, 3, 
                                  (byte) 0xa3, 4, 5, 6, 
                                  0,              
                                  (byte) 0xa3, 1, 2, 4,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0,              
                                  (byte) 0xa3, 1, 2, 5,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0,              
                                  (byte) 0xa3, 1, 2, 6,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0, 0, 0, 0,    
                                  0, 0, 0, 18,    
                                  0, 0, 0, 2};    
        BlockReader block = new BlockReader(new Slice(bytes));
        int count = 0;
        Slice term =  new Slice(new byte[] {2, 3, 4});
        Iterator<KV> kvs = block.find(term);
        while(kvs.hasNext()){
            KV kv = kvs.next();
            count += 1;
            break;
        }
        assertEquals(count, 0);
    }

    public void testBlockFindUnmatchedMiddle() {
        byte[] bytes = new byte[] {0,             
                                  (byte) 0xa3, 1, 2, 3,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0,             
                                  (byte) 0xa3, 1, 2, 4, 
                                  (byte) 0xa3, 4, 5, 6, 
                                  0,             
                                  (byte) 0xa3, 1, 2, 5, 
                                  (byte) 0xa3, 4, 5, 6,  
                                  0,             
                                  (byte) 0xa3, 1, 2, 6,  
                                  (byte) 0xa3, 4, 5, 6,  
                                  0, 0, 0, 0,    
                                  0, 0, 0, 18,    
                                  0, 0, 0, 2};    
        BlockReader block = new BlockReader(new Slice(bytes));
        Slice term =  new Slice(new byte[] {1, 2, 4});
        Iterator<KV> kvs = block.find(new Slice(new byte[] {1, 2, 3, 4}));
        while(kvs.hasNext()){
            KV kv = kvs.next();
            assertEquals(kv.getKey().toString(), term.toString());
            break;
        }
    }
}