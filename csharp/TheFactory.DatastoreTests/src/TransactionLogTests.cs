using System;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {
    [TestFixture]
    public class TransactionLogTests {
        private Random random;

        [SetUp]
        public void SetUp() {
            random = new Random();
        }

        [Test]
        public void TestTransactionLogEmitTransactionFull() {
            var data = new byte[10];
            random.NextBytes(data);

            var stream = new MemoryStream();
            var log = new TransactionLog(stream);

            log.EmitTransaction(data);

            Assert.True(stream.Length == data.Length + TransactionLog.HeaderSize);
            var buf = stream.GetBuffer();

            Assert.True(buf[4] == (byte)TransactionLog.RecordType.Full);
            Assert.True(buf[5] == (byte)0);
            Assert.True(buf[6] == (byte)data.Length);
            Assert.True(buf.CompareBytes(TransactionLog.HeaderSize, data, 0, data.Length));
        }

        [Test]
        public void TestTransactionLogEmitTransactionBoundaryFirst() {
            var data = new byte[100];
            random.NextBytes(data);
            var first = data.Length / 2;
            var lastOffset = TransactionLog.HeaderSize + first;

            var stream = new MemoryStream();
            var log = new TransactionLog(stream);
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
        public void TestTransactionLogEmitTransactionBoundaryFirstNoData() {
            var data = new byte[10];
            random.NextBytes(data);
            var first = 0;
            var lastOffset = TransactionLog.HeaderSize + first;

            var stream = new MemoryStream();
            var log = new TransactionLog(stream);
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
        public void TestTransactionLogEmitTransactionBoundaryPadZeroes() {
            var data = new byte[10];
            random.NextBytes(data);
            var tooSmallBy = 1;
            var recordOffset = TransactionLog.HeaderSize - tooSmallBy;

            var stream = new MemoryStream();
            var log = new TransactionLog(stream);
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
        public void TestTransactionLogEmitTransactionBoundaryManyComplete() {
            var len = TransactionLog.MaxBlockSize * 3;
            var data = new byte[len];
            random.NextBytes(data);

            var stream = new MemoryStream();
            var log = new TransactionLog(stream);

            log.EmitTransaction(data);

            var buf = stream.GetBuffer();

            Assert.True(buf[4] == (byte)TransactionLog.RecordType.First);
            Assert.True(buf[TransactionLog.MaxBlockSize + 4] == (byte)TransactionLog.RecordType.Middle);
            Assert.True(buf[TransactionLog.MaxBlockSize * 2 + 4] == (byte)TransactionLog.RecordType.Middle);
            Assert.True(buf[TransactionLog.MaxBlockSize * 3 + 4] == (byte)TransactionLog.RecordType.Last);
        }

        [Test]
        public void TestTransactionLogEmitTransactionBoundaryMany() {
            var len = TransactionLog.MaxBlockSize * 3;
            var data = new byte[len];
            random.NextBytes(data);
            var first = 10;
            var offset = TransactionLog.HeaderSize + first;

            var stream = new MemoryStream();
            var log = new TransactionLog(stream);
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
}
