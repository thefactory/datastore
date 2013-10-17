using System;
using System.IO;
using TheFactory.Datastore.Helpers;

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
        private byte[] buf = new byte[TransactionLog.MaxBlockSize];
        internal MemoryStream TransactionStream = new MemoryStream();
        private Stream stream;

        public TransactionLogReader(string path) {
            stream = new FileStream(path, FileMode.Open, FileAccess.Read);
        }

        internal TransactionLogReader(Stream stream) {
            this.stream = stream;
        }

        public void Dispose() {
            TransactionStream.Dispose();
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
            var record = ReadRecordHeader();
            stream.Read(buf, 0, record.Length);
            if (record.Checksum != Crc32.ChecksumIeee(buf, 0, record.Length)) {
                throw new FormatException("bad record checksum");
            }
            record.Value = buf;
            return record;
        }

        internal void ReadTransaction() {
            TransactionStream.SetLength(0);  // Reset.

            var record = ReadRecord();
            if (record.Type != TransactionLog.RecordType.Full && record.Type != TransactionLog.RecordType.First) {
                throw new FormatException("unexpected record type");
            }
            TransactionStream.Write(buf, 0, record.Length);

            while (record.Type != TransactionLog.RecordType.Full && record.Type != TransactionLog.RecordType.Last) {
                record = ReadRecord();
                if (record.Type != TransactionLog.RecordType.Middle && record.Type != TransactionLog.RecordType.Last) {
                    throw new FormatException("unexpected record type");
                }
                TransactionStream.Write(buf, 0, record.Length);
            }

            TransactionStream.Seek(0, SeekOrigin.Begin);
        }

        private void ApplyToTablet(MemoryStream transaction, MemoryTablet tablet) {
            // Don't know what this does yet.
            //Console.WriteLine(BitConverter.ToString(transaction.GetBuffer()));
        }

        public long ReplayIntoTablet(MemoryTablet tablet) {
            long count = 0;
            stream.Seek(0, SeekOrigin.Begin);
            var remaining = stream.Length - stream.Position;
            while (remaining >= TransactionLog.HeaderSize) {
                try {
                    ReadTransaction();
                    ApplyToTablet(TransactionStream, tablet);
                    count += 1;

                    // Cue past any padding.
                    var blockRemaining = TransactionLog.MaxBlockSize - (stream.Position % TransactionLog.MaxBlockSize);
                    if (blockRemaining < TransactionLog.HeaderSize) {
                        stream.Seek(blockRemaining, SeekOrigin.Current);
                    }
                } catch (FormatException) {
                    // Cue to the next transaction.
                    var record = ReadRecordHeader();
                    while (record.Type != TransactionLog.RecordType.Full && record.Type != TransactionLog.RecordType.First) {
                        stream.Seek(record.Length, SeekOrigin.Current);
                        remaining = stream.Length - stream.Position;
                        if (remaining < TransactionLog.HeaderSize) {
                            return count;
                        }
                        record = ReadRecordHeader();
                    }
                    // Found the start of a transaction,
                    // rew to it for the next iteration.
                    stream.Seek(-TransactionLog.HeaderSize, SeekOrigin.Current);
                }
                remaining = stream.Length - stream.Position;
            }
            return count;
        }
    }

    internal class TransactionLogWriter : IDisposable {
        internal int Head;
        private byte[] buf;
        private BinaryWriter writer;

        private TransactionLogWriter() {
            Head = 0;
            buf = new byte[TransactionLog.MaxBlockSize];
        }

        internal TransactionLogWriter(Stream stream) : this() {
            writer = new BinaryWriter(stream);
        }

        public TransactionLogWriter(string path) : this() {
            var fs = new FileStream(path, FileMode.Append, FileAccess.Write);
            writer = new BinaryWriter(fs);
        }

        public void Dispose() {
            writer.Dispose();
        }

        private void EmitRecord(byte[] data, TransactionLog.RecordType type, int offset, UInt16 length) {
            // Records take the form of:
            //  checksum: uint32    // crc32c of data[] ; big-endian
            //  type: uint8         // One of FULL, FIRST, MIDDLE, LAST
            //  length: uint16      // big-endian
            //  data: uint8[length]

            var start = Head;

            var checksum = Crc32.ChecksumIeee(data, offset, length);
            var checksumBytes = BitConverter.GetBytes(checksum);
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(checksumBytes);
            }
            Buffer.BlockCopy(checksumBytes, 0, buf, Head, 4);
            Head += 4;

            buf[Head++] = (byte)type;

            var lengthBytes = BitConverter.GetBytes(length);
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(lengthBytes);
            }
            Buffer.BlockCopy(lengthBytes, 0, buf, Head, 2);
            Head += 2;

            Buffer.BlockCopy(data, offset, buf, Head, length);
            Head += length;

            writer.Write(buf, start, Head - start);
        }

        public void EmitTransaction(byte[] data) {
            var type = TransactionLog.RecordType.Full;

            var remaining = data.Length;

            if (Head > TransactionLog.MaxBlockSize - TransactionLog.HeaderSize) {
                // Pad with zeroes and reset.
                var start = Head;

                while (Head < TransactionLog.MaxBlockSize) {
                    buf[Head++] = 0;
                }

                writer.Write(buf, start, Head - start);

                Head = 0;
            }

            while (remaining + TransactionLog.HeaderSize > TransactionLog.MaxBlockSize - Head) {
                type = type == TransactionLog.RecordType.Full ? TransactionLog.RecordType.First : TransactionLog.RecordType.Middle;
                var offset = data.Length - remaining;
                var length = (UInt16)(TransactionLog.MaxBlockSize - Head - TransactionLog.HeaderSize);
                EmitRecord(data, type, offset, length);
                remaining -= length;
                Head = 0;
            }

            type = type == TransactionLog.RecordType.Full ? TransactionLog.RecordType.Full : TransactionLog.RecordType.Last;

            EmitRecord(data, type, data.Length - remaining, (UInt16)remaining);

            writer.Flush();
        }
    }
}
