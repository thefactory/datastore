using System;
using System.IO;
using TheFactory.Datastore.Helpers;
using System.Collections.Generic;

namespace TheFactory.Datastore {
    internal class TransactionLog {
        internal enum RecordType {
            Full = 1,
            First,
            Middle,
            Last
        }

        internal const int MaxBlockSize = 32768;  // Max block size is 32KiB.
        internal const int HeaderSize = 7;  // 4 (checksum) + 1 (type) + 2 (length).

        internal struct Record {
            public UInt32 Checksum;
            public RecordType Type;
            public UInt16 Length;
            public byte[] Value;
        }
    }

    internal class TransactionLogReader : IDisposable {
        private Stream stream;

        public TransactionLogReader(string path) {
            stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read);
        }

        internal TransactionLogReader(Stream stream) {
            this.stream = stream;
        }

        public void Dispose() {
            stream.Dispose();
        }

        private TransactionLog.Record ReadRecordHeader() {
            var record = new TransactionLog.Record();
            record.Checksum = stream.ReadInt();
            record.Type = (TransactionLog.RecordType)stream.ReadByte();
            record.Length = stream.ReadShort();
            return record;
        }

        private TransactionLog.Record ReadRecord() {
            // if the transaction stream has too few bytes to handle a record
            // header, seek to next
            var blockRemaining = TransactionLog.MaxBlockSize - (stream.Position % TransactionLog.MaxBlockSize);
            if (blockRemaining < TransactionLog.HeaderSize) {
                stream.Seek(blockRemaining, SeekOrigin.Current);
            }

            var record = ReadRecordHeader();
            byte[] buf = new byte[record.Length];
            stream.Read(buf, 0, record.Length);
            if (record.Checksum != Crc32.ChecksumIeee(buf, 0, record.Length)) {
                throw new FormatException("bad record checksum");
            }

            record.Value = buf;
            return record;
        }

        internal Slice ReadTransaction(MemoryStream buffer) {
            buffer.SetLength(0);

            var record = ReadRecord();
            if (record.Type != TransactionLog.RecordType.Full && record.Type != TransactionLog.RecordType.First) {
                throw new FormatException("unexpected record type");
            }
            buffer.Write(record.Value, 0, record.Value.Length);

            while (record.Type != TransactionLog.RecordType.Full && record.Type != TransactionLog.RecordType.Last) {
                record = ReadRecord();
                if (record.Type != TransactionLog.RecordType.Middle && record.Type != TransactionLog.RecordType.Last) {
                    throw new FormatException("unexpected record type");
                }
                buffer.Write(record.Value, 0, record.Value.Length);
            }

            return new Slice(buffer.GetBuffer(), 0, (int)buffer.Length);
        }

        public IEnumerable<Slice> Transactions() {
            MemoryStream buffer = new MemoryStream();

            stream.Seek(0, SeekOrigin.Begin);
            while (stream.Length - stream.Position >= TransactionLog.HeaderSize) {
                // can't yield from a try block, so initialize this to null and check below
                Slice transaction = null;
                try {
                    transaction = ReadTransaction(buffer);
                } catch (FormatException) {
                    // skip this record
                }

                if (transaction != null) {
                    yield return transaction;
                }
            }

            yield break;
        }
    }

    internal class TransactionLogWriter : IDisposable {
        Stream output;
        MemoryStream buf;

        private TransactionLogWriter() {
            buf = new MemoryStream(TransactionLog.MaxBlockSize);
        }

        internal TransactionLogWriter(Stream stream) : this() {
            output = stream;
        }

        public TransactionLogWriter(string path) : this() {
            output = new FileStream(path, FileMode.Append, FileAccess.Write);
        }

        public void Dispose() {
            buf.Dispose();
            output.Dispose();
        }

        public int Remaining() {
            return (int)(TransactionLog.MaxBlockSize - (output.Position % TransactionLog.MaxBlockSize));
        }

        private void EmitRecord(Slice rec, TransactionLog.RecordType type) {
            // Records take the form of:
            //  checksum: uint32    // crc32c of data[] ; big-endian
            //  type: uint8         // One of FULL, FIRST, MIDDLE, LAST
            //  length: uint16      // big-endian
            //  data: uint8[length]

            // reset buf to empty
            buf.SetLength(0);

            UInt32 checksum = Crc32.ChecksumIeee(rec.Array, rec.Offset, rec.Length);
            Utils.WriteUInt32(buf, checksum);

            buf.WriteByte((byte)type);

            UInt16 length = (UInt16)rec.Length;
            Utils.WriteUInt16(buf, length);

            buf.Write(rec.Array, rec.Offset, rec.Length);

            output.Write(buf.GetBuffer(), 0, (int)buf.Length);
        }

        public void EmitTransaction(Slice data) {
            int remaining = Remaining();
            if (remaining < TransactionLog.HeaderSize) {
                // there isn't enough room for a record; pad with zeros
                output.Write(new byte[remaining], 0, remaining);
            }

            var type = TransactionLog.RecordType.Full;
            while (data.Length > Remaining()) {
                if (type == TransactionLog.RecordType.Full) {
                    type = TransactionLog.RecordType.First;
                } else {
                    type = TransactionLog.RecordType.Middle;
                }

                var recLen = Remaining() - TransactionLog.HeaderSize;
                EmitRecord(data.Subslice(0, recLen), type);
                data = data.Subslice(recLen);
            }

            if (type != TransactionLog.RecordType.Full) {
                type = TransactionLog.RecordType.Last;
            }

            EmitRecord(data, type);
            output.Flush();
        }
    }
}
