package com.thefactory.datastore;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.Iterator;
import java.util.Arrays;
import java.io.IOException;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;

public class TransactionLog {

    private final FileSystem fileSystem;

    private enum RecordType {
        FULL(1),
        FIRST(2),
        MIDDLE(3),
        LAST(4);        

        public final int code;

        RecordType(int code){
            this.code = code;
        } 

        public static RecordType forCode(int code){
            switch (code) {
                case 1:
                    return FULL;
                case 2: 
                    return FIRST;
                case 3:
                    return MIDDLE;
                case 4:
                    return LAST;
                default:
                    throw new IllegalArgumentException("invalid code for RecordType: " + code);        
            }
        }
    }

    public final static int MAX_BLOCK_SIZE = 32768;  
    public final static int HEADER_SIZE = 7;         

    private static class RecordHeader {
        public final long checksum;
        public final RecordType type;
        public final int length;

        public RecordHeader(long checksum, 
                      RecordType type, 
                      int length){
            this.checksum = checksum;
            this.type = type;
            this.length = length;
        }
    }

    private static class Record {
        public final RecordHeader header;
        public final byte[] value;

        public Record(RecordHeader header, 
                      byte[] value){
            this.header = header;
            this.value = value;
        }
    }

    public TransactionLog(FileSystem fileSystem){
        this.fileSystem = fileSystem;
    }

    public Reader getReader(String transactionLogfile) {
        return new Reader(transactionLogfile);
    }

    public Writer getWriter(String transactionLogfile) {
        return new Writer(transactionLogfile);
    }

    public class Reader {
        private final DatastoreChannel channel;
        private final CRC32 crc32 = new CRC32();
        private long position = 0;
        private final long size;

        public Reader(String transactionLogfile){
            this.channel = fileSystem.open(transactionLogfile);
            this.size = fileSystem.size(transactionLogfile);
        }

        public Iterator<Slice> transactions(){
            return new Iterator<Slice>() {
                public boolean hasNext() {
                    return position + HEADER_SIZE < size;
                }
                
                public Slice next() {
                    try {
                        return readTransaction();
                    } catch (IOException e) {
                        throw new IllegalArgumentException("reading transaction failed: " + e);            
                    }
                }
                    
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        private void readFully(byte[] buffer) throws IOException {
            int read = 0;
            while(read < buffer.length) {
                read += channel.read(ByteBuffer.wrap(buffer, read, buffer.length - read), position + read);
            }
        }

        private RecordHeader readRecordHeader() throws IOException {
            byte[] buffer = new byte[HEADER_SIZE];
            readFully(buffer);
            position += HEADER_SIZE;
            Slice in = new Slice(buffer);

            long checksum = in.readInt();
            int type = in.readByte();
            int len = in.readShort();

            return new RecordHeader(checksum, RecordType.forCode(type), len);
        }

        private Record readRecord() throws IOException {
            // if the transaction stream has too few bytes to handle a record
            // header, seek to next
            long remaining = MAX_BLOCK_SIZE - (position % MAX_BLOCK_SIZE);
            if(remaining < HEADER_SIZE) {
                position += remaining;
            } 

            RecordHeader header = readRecordHeader();
            byte[] buffer = new byte[header.length];
            readFully(buffer);
            position += header.length;
            crc32.reset();
            crc32.update(buffer, 0, header.length);
            if(header.checksum != crc32.getValue()){
                throw new NumberFormatException(String.format("bad record checksum: %02x != %02x", header.checksum, crc32.getValue()));
            }
            return new Record(header, buffer);
        }

        private Slice readTransaction() throws IOException {
            ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
            
            Record record = readRecord();
            if (record.header.type != RecordType.FULL && record.header.type != RecordType.FIRST) {
                throw new IOException("unexpected record type: " + record.header.type.name());
            }

            buffer.writeBytes(record.value, 0, record.value.length);

            while (record.header.type != RecordType.FULL && record.header.type != RecordType.LAST) {
                record = readRecord();
                if (record.header.type != RecordType.MIDDLE && record.header.type != RecordType.LAST) {
                    throw new IOException("unexpected record type: " + record.header.type.name());
                }
                buffer.writeBytes(record.value, 0, record.value.length);
            }

            return new Slice(buffer.array(), 0, buffer.readableBytes());
        }
    }

    public class Writer {
        private final DatastoreChannel channel;
        private final CRC32 crc32 = new CRC32();
        private int position = 0;


        public Writer(String transactionLogfile){
            this.channel = fileSystem.create(transactionLogfile);
        }

        public void writeTransaction(Slice data) throws IOException {
            int remaining = remaining();
            if (remaining < HEADER_SIZE) {
                // there isn't enough room for a record; pad with zeros
                byte[] padding = new byte[remaining];
                Arrays.fill(padding, (byte) 0);
                channel.write(ByteBuffer.wrap(padding));
                position += remaining;
            }

            RecordType type = RecordType.FULL;
            while (data.getLength() > remaining()) {
                if (type == RecordType.FULL) {
                    type = RecordType.FIRST;
                } else {
                    type = RecordType.MIDDLE;
                }

                int recLen = remaining() - HEADER_SIZE;
                writeRecord(data.subslice(0, (int)recLen), type);
                data = data.subslice(recLen);
            }

            if (type != RecordType.FULL) {
                type = RecordType.LAST;
            }

            writeRecord(data, type);
        }

        public int remaining() {
            return MAX_BLOCK_SIZE - (position % MAX_BLOCK_SIZE);
        }

        private void writeRecord(Slice record, RecordType type) throws IOException {
            byte[] bytes = new byte[HEADER_SIZE];
            ByteBuffer header = ByteBuffer.wrap(bytes);
            crc32.reset();
            crc32.update(record.array, record.getOffset(), record.getLength());
            header.putInt(0, (int) crc32.getValue());
            header.put(4, (byte) type.code);
            header.putShort(5, (short) record.getLength());
            channel.write(header);
            channel.write(ByteBuffer.wrap(record.array, record.getOffset(), record.getLength()));
            position += HEADER_SIZE + record.getLength();
        }
    }
}
