package com.thefactory.datastore;

import java.io.Closeable;
import java.io.IOException;
import java.io.Closeable;
import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.lang.Override;
import java.lang.Thread;
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 


public class Database implements Closeable {
    public final Options options;
    private final Tablets tablets = new Tablets();
    private final FileManager fileManager;
    private Closeable lock = null;
    private TransactionLog.Writer transactionLogWriter = null;
    private Log log = LogFactory.getLog(Database.class);

    public static class Options {
        public final FileSystem fileSystem;
        public final long maxMutableTabletSize;

        public boolean deleteOnClose;
        public boolean createIfMissing;
        public boolean verifyChecksums;

        public Options() {
            createIfMissing = true;
            verifyChecksums = false;
            deleteOnClose = false;
            fileSystem = new DiskFileSystem();
            maxMutableTabletSize = 1024 * 1024 * 4;
        }

        public Options(final FileSystem fileSystem) {
            createIfMissing = true;
            verifyChecksums = false;
            deleteOnClose = false;
            this.fileSystem = fileSystem;
            maxMutableTabletSize = 1024 * 1024 * 4;
        }

        public Options(final FileSystem fileSystem,
                       final long maxMutableTabletSize) {
            this.fileSystem = fileSystem;
            this.maxMutableTabletSize = maxMutableTabletSize;
            this.createIfMissing = true;
            this.verifyChecksums = false;
            this.deleteOnClose = false;
        }
    }

    public interface KVPredicate {
        boolean evaluate(final KV kv);
    }

    private class Tablets {
        public final Deque<String> stack = new LinkedBlockingDeque<String>();
        public final Deque<FileTablet> file = new LinkedBlockingDeque<FileTablet>();        
        public MemoryTablet mutable = new MemoryTablet();
        public MemoryTablet saving = null;
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

    public static Database openTmp() throws IOException {
        Options options = new Options();
        options.deleteOnClose = true;
        return open(Utils.createTempDirectory("db-").toString(), options);
    }

    public static Database openTablets(final Iterable<File> tablets) throws IOException {
        Database db = openTmp();
        for(File tablet : tablets) {
            String destTabletName = UUID.randomUUID().toString();
            Utils.fileCopy(new File(db.fileManager.dir, destTabletName), tablet);
            db.pushTablet(destTabletName);
        }  
        return db;
    }

    public void pushTablet(String name) throws IOException {
        synchronized(tablets){
            DatastoreChannel tabletChannel = options.fileSystem.open(fileManager.dbFilename(name));
            tablets.file.addLast(new FileTablet(tabletChannel, new TabletReaderOptions()));
            tablets.stack.addLast(name);
        }    

        fileManager.writeTabletFilenames(tablets.stack);
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

        if(options.deleteOnClose) {
            Utils.deletePathRecursive(new File(fileManager.dir));
        }
    }

    public Iterator<KV> find() throws IOException {
        return find(null);
    }

    public Iterator<KV> findByPrefix(final Slice term) throws IOException {
        return findWhile(term, new KVPredicate() {
            public boolean evaluate(final KV kv) {
                return Slice.isPrefix(kv.getKey(), term);
            }
        });
    }

    public Iterator<KV> find(final Slice term) throws IOException {
        return findWhile(term, new KVPredicate() {
            public boolean evaluate(final KV kv) {
                return true;
            }
        });
    }

    public Iterator<KV> findWhile(final Slice term, final KVPredicate predicate) throws IOException {

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
                int priority = 0;
                Iterator<FileTablet> it = tablets.file.iterator();
                while(it.hasNext()){
                    enqueueNextItem(it.next().find(term), priority++);
                }
                if(tablets.saving != null) {
                    enqueueNextItem(tablets.saving.find(term), priority++);                    
                }
                enqueueNextItem(tablets.mutable.find(term), priority);

                current = pop();
                if((current != null) && (!predicate.evaluate(current))) {
                    current = null;
                }
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
                if((current != null) && (!predicate.evaluate(current))) {
                    current = null;
                }

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
        transactionLogWriter = new TransactionLog(options.fileSystem).getWriter(transactionLogPath, 
            options.fileSystem.exists(transactionLogPath));
        
        tablets.mutable = fromLogOrElse(transactionLogPath, new MemoryTablet());
        tablets.saving = fromLogOrElse(fileManager.getSecondaryTransactionLog(), null);

        Collection<String> fileTablets = fileManager.loadTabletFilenames();
        for(String fileTablet : fileTablets) {
            pushTablet(fileTablet);
        }
    }

    private MemoryTablet fromLogOrElse(final String transactionLogPath, final MemoryTablet tablet) {
        MemoryTablet ret = tablet;
        if(!options.fileSystem.exists(transactionLogPath)) {
            return ret;
        }

        if(ret == null) {
            ret = new MemoryTablet();
        }

        TransactionLog.Reader reader = new TransactionLog(options.fileSystem).getReader(transactionLogPath);
        Iterator<Slice> iterator = reader.transactions();
        while(iterator.hasNext()){
            ret.apply(Batch.wrap(iterator.next()));            
        }       
        return ret; 
    }

    private synchronized void apply(Batch batch) throws IOException {
        transactionLogWriter.writeTransaction(batch.asSlice());
        tablets.mutable.apply(batch);    

        if(shouldSave() && canSave()){
            save();
        }
    }

    private Boolean canSaveFlag = Boolean.TRUE;
    
    public boolean canSave() {
        synchronized (canSaveFlag) {
            while (canSaveFlag.booleanValue() == false) {
                try {
                    canSaveFlag.wait();
                } catch (InterruptedException e) {
                    return false;
                } 
            }
        }
        return true;
    }

    public void signalSaveComplete() {
        synchronized (canSaveFlag) {
            canSaveFlag = Boolean.TRUE;
            canSaveFlag.notifyAll();
        }
    }

    private boolean shouldSave() {
        synchronized(tablets) {
            return (tablets.saving == null) && (tablets.mutable.size() > options.maxMutableTabletSize);
        }
    }

    private void save() throws IOException {
        if(canSave() && !shouldSave()) {
            return;
        }

        transactionLogWriter.close();
        options.fileSystem.rename(fileManager.getTransactionLog(), fileManager.getSecondaryTransactionLog());
        transactionLogWriter = new TransactionLog(options.fileSystem).getWriter(fileManager.getTransactionLog(), false);        

        synchronized(tablets){
            tablets.saving = tablets.mutable;
            tablets.mutable = new MemoryTablet();
        }

        new Thread() {
            public void run() {
                String name = UUID.randomUUID().toString();
                try {
                    writeTablet(name);
                    if(fileManager.exists(name, 10)) {
                        pushTablet(name);
                        synchronized(tablets) {
                            tablets.saving = null;     
                        }                            
                        options.fileSystem.remove(fileManager.getSecondaryTransactionLog());
                    } else {
                        log.error(String.format("Failed to save tablet: %s", name));                        
                    }
                } catch (IOException e) {
                    log.error(String.format("Flushing tablet failed with %s", e));
                } finally {
                   signalSaveComplete(); 
                }            
                log.debug(String.format("Successfully flushed tablet (%s)", name));
            }
        }.start();
    }

    private void writeTablet(String name) throws IOException {
        TabletWriter writer = new TabletWriter(new TabletWriterOptions());
        DatastoreChannel channel = options.fileSystem.create(fileManager.dbFilename(name));
        writer.writeTablet(channel, tablets.saving.find());
    }
}

