using System;
using System.IO;

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

        internal int Head;
        private byte[] buf;
        private BinaryWriter writer;

        private TransactionLog() {
            Head = 0;
            buf = new byte[MaxBlockSize];
        }

        internal TransactionLog(Stream stream) : this() {
            writer = new BinaryWriter(stream);
        }

        public TransactionLog(string path) : this() {
            var fs = new FileStream(path, FileMode.Append, FileAccess.Write);
            writer = new BinaryWriter(fs);
        }

        private void EmitRecord(byte[] data, RecordType type, int offset, UInt16 length) {
            // Records take the form of:
            //  checksum: uint32    // crc32c of data[] ; big-endian
            //  type: uint8         // One of FULL, FIRST, MIDDLE, LAST
            //  length: uint16      // big-endian
            //  data: uint8[length]

            var start = Head;

            var checksum = Crc32.ChecksumIeee(data);
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
            var type = RecordType.Full;

            var remaining = data.Length;

            if (Head > MaxBlockSize - HeaderSize) {
                // Pad with zeroes and reset.
                var start = Head;

                while (Head < MaxBlockSize) {
                    buf[Head++] = 0;
                }

                writer.Write(buf, start, Head - start);

                Head = 0;
            }

            while (remaining + HeaderSize > MaxBlockSize - Head) {
                type = type == RecordType.Full ? RecordType.First : RecordType.Middle;
                var offset = data.Length - remaining;
                var length = (UInt16)(MaxBlockSize - Head - HeaderSize);
                EmitRecord(data, type, offset, length);
                remaining -= length;
                Head = 0;
            }

            type = type == RecordType.Full ? RecordType.Full : RecordType.Last;

            EmitRecord(data, type, data.Length - remaining, (UInt16)remaining);

            writer.Flush();
        }
    }
}
