package com.thefactory.datastore;

import java.io.Closeable;
import java.io.IOException;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Comparator;
import java.util.UUID;
import java.lang.Override;


public class Database implements Closeable {
    public final Options options;
    private final Tablets tablets = new Tablets();
    private final FileManager fileManager;
    private Closeable lock = null;
    private TransactionLog.Writer transactionLogWriter = null;

    public static class Options {
        public final boolean createIfMissing;
        public final boolean verifyChecksums;
        public final FileSystem fileSystem;
        public final long maxMutableTabletSize;

        public Options() {
            createIfMissing = true;
            verifyChecksums = false;
            fileSystem = new DiskFileSystem();
            maxMutableTabletSize = 1024 * 1024 * 4;
        }

        public Options(final FileSystem fileSystem) {
            createIfMissing = true;
            verifyChecksums = false;
            this.fileSystem = fileSystem;
            maxMutableTabletSize = 1024 * 1024 * 4;
        }

        public Options(final FileSystem fileSystem,
                       final boolean createIfMissing, 
                       final boolean verifyChecksums, 
                       final long maxMutableTabletSize) {
            this.createIfMissing = createIfMissing;
            this.verifyChecksums = verifyChecksums;
            this.fileSystem = fileSystem;
            this.maxMutableTabletSize = maxMutableTabletSize;
        }
    }

    private class Tablets {
        public final Deque<String> stack = new ArrayDeque<String>();
        public final Map<String, FileTablet> file = new HashMap<String, FileTablet>();        

        public MemoryTablet mutable = new MemoryTablet();
    }

    private Database(final String path, final Options options) {
        this.options = options;
        this.fileManager = new FileManager(path, options.fileSystem, options.createIfMissing);
    }

    public static Database open(final String path, final Options options) throws IOException {
        Database database = new Database(path, options);
        database.open();
        return database;
    }

    public static Database open(final String path) throws IOException {
        return open(path, new Options());
    }

    public void pushTablet(String name) throws IOException {
        DatastoreChannel tabletChannel = options.fileSystem.open(fileManager.dbFilename(name));
        tablets.file.put(name, new FileTablet(tabletChannel, new TabletReaderOptions()));
        tablets.stack.addLast(name);
        fileManager.writeTabletFilenames(tablets.stack);
    }

    public void flush() throws IOException {
        String name = UUID.randomUUID().toString();
        save(name);
        tablets.mutable = new MemoryTablet();
        pushTablet(name); 
    }

    public Slice get(Slice key) throws KeyNotFoundException, IOException {
        Iterator<KV> kvs = find(key);        
        if(!kvs.hasNext()) {
            throw new KeyNotFoundException(key.toUTF8String());
        }
        KV kv = kvs.next();
        if((Slice.compare(kv.getKey(), key) == 0) && (!kv.isDeleted())) {
            return kv.getValue().detach();
        }        
        throw new KeyNotFoundException(key.toUTF8String());
    }

    public Slice getOrElse(Slice key, Slice def) throws IOException {
        Iterator<KV> kvs = find(key);        
        if(!kvs.hasNext()) {
            return def;
        }
        KV kv = kvs.next();
        if((Slice.compare(kv.getKey(), key) == 0) && (!kv.isDeleted())) {
            return kv.getValue().detach();
        }        
        return def;
    }

    public void put(Slice key, Slice value) throws IOException  {
        Batch batch = new Batch();
        batch.put(key, value);
        apply(batch);
    }

    public void delete(Slice key) throws IOException  {
        Batch batch = new Batch();
        batch.delete(key);
        apply(batch);
    }


    @Override
    public void close() throws IOException {
        if (transactionLogWriter != null) {
            transactionLogWriter.close();
            transactionLogWriter = null;
        }

        if (lock != null) {
            lock.close();
            lock = null;
        }
    }

    public Iterator<KV> find() throws IOException {
        return find(null);
    }

    public Iterator<KV> find(final Slice term) throws IOException {

        return new Iterator<KV>() {
            class QueueItem {
                public final int priority;
                public final Iterator<KV> iterator;
                public final KV kv;

                public QueueItem(final Iterator<KV> iterator, final KV kv, final int priority){
                    this.kv = kv;
                    this.priority = priority;
                    this.iterator = iterator;
                }
            }

            private TreeSet<QueueItem> queue = new TreeSet<QueueItem> (
                new Comparator<QueueItem>() {
                    public int compare(QueueItem x, QueueItem y) {
                        int ret = Slice.compare(x.kv.getKey(), y.kv.getKey());
                        if (ret != 0) {
                            return ret;
                        }
                        return y.priority - x.priority;
                    }
                }
            );

            private KV current = null;

            {
                Iterator<String> tabletIterator = tablets.stack.iterator();
                Iterator<KV> kvIterator;
                QueueItem item;
                int priority = 0;
                while(tabletIterator.hasNext()){
                    String name = tabletIterator.next();
                    kvIterator = tablets.file.get(name).find(term);
                    if (kvIterator.hasNext()) {
                        enqueueNextItem(kvIterator, priority++);
                    }                    
                }
                kvIterator = tablets.mutable.find(term);
                enqueueNextItem(kvIterator, priority);
                current = pop();
            }

            @Override
            public boolean hasNext() {
                return (current != null);
            }

            @Override
            public KV next() {
                if(current == null) {
                    throw new NoSuchElementException("iterator reached end");
                }
                KV ret = current;
                current = pop();
                return ret;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private KV pop() {
                while(true) {
                    KV ret = nextKey();
                    if (ret == null) {
                        return null;
                    }
                    if (ret.isDeleted()) {
                        continue;
                    }
                    return ret;
                }
            }

            private KV nextKey() {
                KV ret = nextQueueItem();
                if(ret == null){
                    return null;
                }
                while((queue.size() > 0) && ((Slice.compare(queue.first().kv.getKey(), ret.getKey()) == 0))){
                    nextQueueItem();
                }
                return ret;
            }

            private KV nextQueueItem() {
                if(queue.size() == 0) {
                    return null;
                }
                QueueItem item = queue.pollFirst();
                KV ret = item.kv.detach();
                enqueueNextItem(item.iterator, item.priority);
                return ret;
            }

            private void enqueueNextItem(Iterator<KV> iterator, int priority) {
                if(iterator.hasNext()){
                    queue.add(new QueueItem(iterator, iterator.next(), priority)); 
                }  
            }
        };
    }

    private void open() throws IOException {
        lock = options.fileSystem.lock(fileManager.getLockFile());

        String transactionLogPath = fileManager.getTransactionLog();
        if(fileManager.exists(transactionLogPath)) {
            TransactionLog.Reader reader = new TransactionLog(options.fileSystem).getReader(transactionLogPath);
            Iterator<Slice> iterator = reader.transactions();
            while(iterator.hasNext()){
                tablets.mutable.apply(Batch.wrap(iterator.next()));
            }
            transactionLogWriter = new TransactionLog(options.fileSystem).getWriter(transactionLogPath, true);
        } else {
            transactionLogWriter = new TransactionLog(options.fileSystem).getWriter(transactionLogPath, false);
        }

        Collection<String> fileTablets = fileManager.loadTabletFilenames();
        for(String fileTablet : fileTablets) {
            pushTablet(fileTablet);
        }
    }

    private void apply(Batch batch) throws IOException {
        synchronized(this) {
            transactionLogWriter.writeTransaction(batch.asSlice());
            tablets.mutable.apply(batch);
            if(tablets.mutable.size() < options.maxMutableTabletSize){
                return;
            }
            flush();
        }
    }

    private void save(String name) throws IOException {
        Iterator<KV> kvs = tablets.mutable.find();
        TabletWriter writer = new TabletWriter(new TabletWriterOptions());
        DatastoreChannel channel = options.fileSystem.create(fileManager.dbFilename(name));
        writer.writeTablet(channel, kvs);
    }
}
