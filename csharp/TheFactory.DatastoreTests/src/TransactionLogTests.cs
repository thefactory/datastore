using System;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {
    [TestFixture]
    public class TransactionLogReaderTests {
        [Test]
        public void TestTransactionLogReaderSingle() {
            var bytes = new byte[] { 0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x01,                      // type (Full).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1 };
            var stream = new MemoryStream(bytes);

            var reader = new TransactionLogReader(stream);
            reader.ReadTransaction();
            var data = reader.TransactionStream;
            Assert.True(bytes.CompareBytes(TransactionLog.HeaderSize, data.GetBuffer(), 0, (int)data.Length));
        }

        [Test]
        public void TestTransactionLogReaderSingleFirstLast() {
            var bytes = new byte[] { 0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x02,                      // type (First).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1,
                                     0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x04,                      // type (Last).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1 };
            var stream = new MemoryStream(bytes);

            var reader = new TransactionLogReader(stream);
            reader.ReadTransaction();
            var data = reader.TransactionStream;
            Assert.True(bytes.CompareBytes(TransactionLog.HeaderSize, data.GetBuffer(), 0, 10));
            Assert.True(bytes.CompareBytes(TransactionLog.HeaderSize * 2 + 10, data.GetBuffer(), 10, 10));
        }

        [Test]
        public void TestTransactionLogReaderSingleFirstMiddleLast() {
            var bytes = new byte[] { 0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x02,                      // type (First).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1,
                                     0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x03,                      // type (Middle).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1,
                                     0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x04,                      // type (Last).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1 };
            var stream = new MemoryStream(bytes);

            var reader = new TransactionLogReader(stream);
            reader.ReadTransaction();
            var data = reader.TransactionStream;
            Assert.True(bytes.CompareBytes(TransactionLog.HeaderSize, data.GetBuffer(), 0, 10));
            Assert.True(bytes.CompareBytes(TransactionLog.HeaderSize * 2 + 10, data.GetBuffer(), 10, 10));
            Assert.True(bytes.CompareBytes(TransactionLog.HeaderSize * 3 + 20, data.GetBuffer(), 20, 10));
        }

        [Test]
        [ExpectedException(typeof(FormatException))]
        public void TestTransactionLogReaderSingleBadRecordType() {
            var bytes = new byte[] { 0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x03,                      // type (Middle).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1 };
            var stream = new MemoryStream(bytes);

            var reader = new TransactionLogReader(stream);
            reader.ReadTransaction();
        }

        [Test]
        [ExpectedException(typeof(FormatException))]
        public void TestTransactionLogReaderSingleBadSecondRecordType() {
            var bytes = new byte[] { 0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x02,                      // type (First).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1,
                                     0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x01,                      // type (Full).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1 };
            var stream = new MemoryStream(bytes);

            var reader = new TransactionLogReader(stream);
            reader.ReadTransaction();
        }

        [Test]
        [ExpectedException(typeof(FormatException))]
        public void TestTransactionLogReaderSingleBadChecksum() {
            var bytes = new byte[] { 0xB2, 0x16, 0x3A, 0x00,    // checksum (bad).
                                     0x01,                      // type (Full).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1 };
            var stream = new MemoryStream(bytes);

            var reader = new TransactionLogReader(stream);
            reader.ReadTransaction();
        }

        [Test]
        public void TestTransactionLogReaderReplaySingle() {
            var bytes = new byte[] { 0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x01,                      // type (Full).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1 };
            var stream = new MemoryStream(bytes);

            var reader = new TransactionLogReader(stream);
            var count = reader.ReplayIntoTablet(new MemoryTablet());
            Assert.True(count == 1);
        }

        [Test]
        public void TestTransactionLogReaderReplayFirstBad() {
            var bytes = new byte[] { 0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x04,                      // type (Last).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1,
                                     0xB2, 0x16, 0x3A, 0xFF,    // checksum.
                                     0x01,                      // type (Full).
                                     0x00, 0x0A,                // length.
                                     0x82, 0xD1, 0xAF, 0x84, 0x29, 0xD1, 0x04, 0x58, 0x67, 0xC1 };
            var stream = new MemoryStream(bytes);

            var reader = new TransactionLogReader(stream);
            var count = reader.ReplayIntoTablet(new MemoryTablet());
            Assert.True(count == 1);
        }
    }

    [TestFixture]
    public class TransactionLogWriterTests {
        private Random random;

        [SetUp]
        public void SetUp() {
            random = new Random();
        }

        [Test]
        public void TestTransactionLogWriterEmitTransactionFull() {
            var data = new byte[10];
            random.NextBytes(data);

            var stream = new MemoryStream();
            var log = new TransactionLogWriter(stream);

            log.EmitTransaction(data);

            Assert.True(stream.Length == data.Length + TransactionLog.HeaderSize);
            var buf = stream.GetBuffer();

            Assert.True(buf[4] == (byte)TransactionLog.RecordType.Full);
            Assert.True(buf[5] == (byte)0);
            Assert.True(buf[6] == (byte)data.Length);
            Assert.True(buf.CompareBytes(TransactionLog.HeaderSize, data, 0, data.Length));
        }

        [Test]
        public void TestTransactionLogWriterEmitTransactionBoundaryFirst() {
            var data = new byte[100];
            random.NextBytes(data);
            var first = data.Length / 2;
            var lastOffset = TransactionLog.HeaderSize + first;

            var stream = new MemoryStream();
            var log = new TransactionLogWriter(stream);
            // Pretend we're near the end of the block by moving the Head pointer.
            log.Head = TransactionLog.MaxBlockSize - TransactionLog.HeaderSize - first;

            log.EmitTransaction(data);

            var buf = stream.GetBuffer();

            Assert.True(buf[4] == (byte)TransactionLog.RecordType.First);
            Assert.True(buf[5] == (byte)0);
            Assert.True(buf[6] == (byte)first);
            Assert.True(buf.CompareBytes(TransactionLog.HeaderSize, data, 0, first));

            Assert.True(buf[lastOffset + 4] == (byte)TransactionLog.RecordType.Last);
            Assert.True(buf[lastOffset + 5] == (byte)0);
            Assert.True(buf[lastOffset + 6] == (byte)(data.Length - first));
            Assert.True(buf.CompareBytes(lastOffset + TransactionLog.HeaderSize, data, first, data.Length - first));
        }

        [Test]
        public void TestTransactionLogWriterEmitTransactionBoundaryFirstNoData() {
            var data = new byte[10];
            random.NextBytes(data);
            var first = 0;
            var lastOffset = TransactionLog.HeaderSize + first;

            var stream = new MemoryStream();
            var log = new TransactionLogWriter(stream);
            // Pretend we're near the end of the block by moving the Head pointer.
            log.Head = TransactionLog.MaxBlockSize - TransactionLog.HeaderSize;

            log.EmitTransaction(data);

            var buf = stream.GetBuffer();

            Assert.True(buf[4] == (byte)TransactionLog.RecordType.First);
            Assert.True(buf[5] == (byte)0);
            Assert.True(buf[6] == (byte)first);
            Assert.True(buf.CompareBytes(TransactionLog.HeaderSize, data, 0, first));

            Assert.True(buf[lastOffset + 4] == (byte)TransactionLog.RecordType.Last);
            Assert.True(buf[lastOffset + 5] == (byte)0);
            Assert.True(buf[lastOffset + 6] == (byte)(data.Length - first));
            Assert.True(buf.CompareBytes(lastOffset + TransactionLog.HeaderSize, data, first, data.Length - first));
        }

        [Test]
        public void TestTransactionLogWriterEmitTransactionBoundaryPadZeroes() {
            var data = new byte[10];
            random.NextBytes(data);
            var tooSmallBy = 1;
            var recordOffset = TransactionLog.HeaderSize - tooSmallBy;

            var stream = new MemoryStream();
            var log = new TransactionLogWriter(stream);
            // Pretend we're near the end of the block by moving the Head pointer.
            log.Head = TransactionLog.MaxBlockSize - recordOffset;

            log.EmitTransaction(data);

            var buf = stream.GetBuffer();

            for (var i = 0; i < recordOffset; i++) {
                Assert.True(buf[i] == 0);
            }

            Assert.True(buf[recordOffset + 4] == (byte)TransactionLog.RecordType.Full);
            Assert.True(buf[recordOffset + 5] == (byte)0);
            Assert.True(buf[recordOffset + 6] == (byte)data.Length);
            Assert.True(buf.CompareBytes(recordOffset + TransactionLog.HeaderSize, data, 0, data.Length));
        }

        [Test]
        public void TestTransactionLogWriterEmitTransactionBoundaryManyComplete() {
            var len = TransactionLog.MaxBlockSize * 3;
            var data = new byte[len];
            random.NextBytes(data);

            var stream = new MemoryStream();
            var log = new TransactionLogWriter(stream);

            log.EmitTransaction(data);

            var buf = stream.GetBuffer();

            Assert.True(buf[4] == (byte)TransactionLog.RecordType.First);
            Assert.True(buf[TransactionLog.MaxBlockSize + 4] == (byte)TransactionLog.RecordType.Middle);
            Assert.True(buf[TransactionLog.MaxBlockSize * 2 + 4] == (byte)TransactionLog.RecordType.Middle);
            Assert.True(buf[TransactionLog.MaxBlockSize * 3 + 4] == (byte)TransactionLog.RecordType.Last);
        }

        [Test]
        public void TestTransactionLogWriterEmitTransactionBoundaryMany() {
            var len = TransactionLog.MaxBlockSize * 3;
            var data = new byte[len];
            random.NextBytes(data);
            var first = 10;
            var offset = TransactionLog.HeaderSize + first;

            var stream = new MemoryStream();
            var log = new TransactionLogWriter(stream);
            // Pretend we're near the end of the block by moving the Head pointer.
            log.Head = TransactionLog.MaxBlockSize - TransactionLog.HeaderSize - first;

            log.EmitTransaction(data);

            var buf = stream.GetBuffer();

            Assert.True(buf[4] == (byte)TransactionLog.RecordType.First);
            Assert.True(buf.CompareBytes(TransactionLog.HeaderSize, data, 0, first));

            Assert.True(buf[offset + 4] == (byte)TransactionLog.RecordType.Middle);
            offset += TransactionLog.MaxBlockSize;

            Assert.True(buf[offset + 4] == (byte)TransactionLog.RecordType.Middle);
            offset += TransactionLog.MaxBlockSize;

            Assert.True(buf[offset + 4] == (byte)TransactionLog.RecordType.Middle);
            offset += TransactionLog.MaxBlockSize;

            Assert.True(buf[offset + 4] == (byte)TransactionLog.RecordType.Last);
        }
    }

    [TestFixture]
    public class TransactionLogIntegrationTests {
        private Random random;
        private MemoryTablet tablet;

        [SetUp]
        public void SetUp() {
            random = new Random();
            tablet = new MemoryTablet();
        }

        private long TransactionLogIntegration(int size, int count) {
            var bytes = new byte[size * count];
            random.NextBytes(bytes);
            var stream = new MemoryStream();
            var writer = new TransactionLogWriter(stream);

            // Write random log.
            var buf = new byte[size];
            for (var i = 0; i < count; i++) {
                Buffer.BlockCopy(bytes, i * size, buf, 0, size);
                writer.EmitTransaction(buf);
            }

            // Seek to the start.
            stream.Seek(0, SeekOrigin.Begin);

            // Read back out.
            var reader = new TransactionLogReader(stream);
            return reader.ReplayIntoTablet(tablet);
        }

        [Test]
        public void TestTransactionLogIntegration0x4700() {
            // (0 + 7) * 4700 = 32900 (will have > 1 block).
            // 32768 / 7.0 = 4681.14 (a transaction will cross block).
            // 32768 - 7 * 4681 = 32767 (will cause zero padding).
            var size = 0;
            var count = 4700;
            var result = TransactionLogIntegration(size, count);
            Assert.True(result == count);
        }

        [Test]
        public void TestTransactionLogIntegration100x310() {
            // (100 + 7) * 310 = 33170 (will have > 1 block).
            // 32768 / 107.0 = 306.24 (a transaction will cross a block).
            // 32768 - 7 * 310 = 30598 (no zero padding).
            var size = 100;
            var count = 310;
            var result = TransactionLogIntegration(size, count);
            Assert.True(result == count);
        }
    }
}
