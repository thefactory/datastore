using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using MsgPack;
using Snappy.Sharp;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {
    internal interface ITablet {
        void Close();

        IEnumerable<IKeyValuePair> Find();

        IEnumerable<IKeyValuePair> Find(byte[] term);
    }

    internal class MemoryTablet : ITablet {
        private SortedSet<IKeyValuePair> backing;
        private ReaderWriterLockSlim backingLock;

        // Deleted key marker -- consumers of Get() and Find() enumerator
        // should check against this value and take appropriate action.
        public const byte[] Tombstone = null;

        public MemoryTablet() {
            var cmp = new MemoryTabletKeyComparer();
            backing = new SortedSet<IKeyValuePair>(cmp);
            backingLock = new ReaderWriterLockSlim();
        }

        public void Set(byte[] key, byte[] val) {
            // Have to remove and re-add for SortedSet backing.
            var pair = new MemoryKeyValuePair(key, val);
            backingLock.EnterWriteLock();
            try {
                backing.Remove(pair);
                backing.Add(pair);
            } finally {
                backingLock.ExitWriteLock();
            }
        }

        public void Delete(byte[] key) {
            Set(key, Tombstone);
        }

        public void Close() {
            backing.Clear();
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find(null);
        }

        public IEnumerable<IKeyValuePair> Find(byte[] term) {
            if (backing.Count == 0) {
                yield break;
            }

            backingLock.EnterReadLock();
            try {
                var set = backing;

                if (term != null) {
                    if (backing.Max.Key.CompareKey(term) < 0) {
                        // Max key is less than the search term -- empty.
                        yield break;
                    }
                    var searchTerm = new MemoryKeyValuePair(term, null);
                    set = backing.GetViewBetween(searchTerm, backing.Max);
                }

                foreach (var p in set) {
                    yield return p;
                }

                yield break;
            } finally {
                backingLock.ExitReadLock();
            }
        }

        private class MemoryKeyValuePair : IKeyValuePair {
            public byte[] Key { get; private set; }
            public byte[] Value { get; private set; }
            public bool IsDeleted {
                get {
                    return ReferenceEquals(Value, Tombstone);
                }
            }

            public MemoryKeyValuePair(byte[] key, byte[] val) {
                Key = key;
                Value = val;
            }
        }

        private class MemoryTabletKeyComparer : IComparer<IKeyValuePair> {
            public int Compare(IKeyValuePair x, IKeyValuePair y) {
                if (ReferenceEquals(x, y)) {
                    return 0;
                }
                return x.Key.CompareKey(y.Key);
            }
        }
    }

    internal class Constants {
        public const UInt32 TabletMagic = 0x0b501e7e;
        public const UInt32 MetaIndexMagic = 0x0ea7da7a;
        public const UInt32 DataIndexMagic = 0xda7aba5e;
    }

    internal class FileTablet : ITablet {
        private Stream stream;
        private SnappyDecompressor decompressor;
        private List<TabletIndexRecord> dataIndex, metaIndex;

        public FileTablet(Stream stream) {
            this.stream = stream;
            decompressor = new SnappyDecompressor();
        }

        public void Close() {
            stream.Close();
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find(null);
        }

        public IEnumerable<IKeyValuePair> Find(byte[] term) {
            // Load indexes if we have no dataIndex.
            if (dataIndex == null) {
                var footer = LoadFooter();
                metaIndex = LoadIndex(footer.MetaIndexOffset, footer.MetaIndexLength, Constants.MetaIndexMagic);
                dataIndex = LoadIndex(footer.DataIndexOffset, footer.DataIndexLength, Constants.DataIndexMagic);
            }

            int blockIndex = 0;

            if (term != null && term.Length != 0) {
                // Generate an artificial TabletIndexRecord with term as Data.
                var termIndexRecord = new TabletIndexRecord();
                termIndexRecord.Data = term;

                // Find and load the correct block for term.
                var comparer = new TabletIndexRecordDataComparer();
                blockIndex = dataIndex.BinarySearch(termIndexRecord, comparer);
                if (blockIndex < 0) {
                    // If we're less than all values, we'll get ~0, so index
                    // should be 0. All other cases should result in ~index - 1.
                    blockIndex = ~blockIndex == 0 ? 0 : ~blockIndex - 1;
                }
            }

            // Iterator of everything left in the tablet.
            for (var i = blockIndex; i < dataIndex.Count ; i++) {
                var block = LoadBlock(dataIndex[i].Offset);
                foreach (var p in block.Find(term)) {
                    yield return p;
                }
            }

            yield break;
        }

        internal Block LoadBlock(long offset) {
            //
            // In a tablet, a block is preceeded by:
            //   [ checksum (msgpack uint32)          ]
            //   [ type/compression (msgpack fixpos)  ]
            //   [ length (msgpack uint32)            ]
            //
            stream.Seek(offset, SeekOrigin.Begin);
            var checksum = Unpacking.UnpackObject(stream).AsUInt32();
            var type = Unpacking.UnpackObject(stream).AsInt32();
            var length = Unpacking.UnpackObject(stream).AsInt32();

            // Read (maybe compressed) block-data into memory.
            var buf = new byte[length];
            stream.Read(buf, 0, length);

            if (checksum != 0 && checksum != Crc32.ChecksumIeee (buf)) {
                var msg = String.Format("Bad checksum for block at {0}", offset);
                throw new TabletValidationException(msg);
            }


            byte[] data;

            switch (type & 1) {  // compression is lowest order bit.
                case 0:  // uncompressed.
                    data = buf;
                    break;
                case 1:  // snappy.
                    // XXX: SnappyDecompressor doesn't operate on a stream,
                    // so we're possibly doubling up on memory here.
                    data = decompressor.Decompress(buf, 0, length);
                    break;
                default:
                    var msg = String.Format("Unknown block type {0}", type);
                    throw new TabletValidationException(msg);
            }

            return new Block(data, 0, data.Length);
        }

        internal List<TabletIndexRecord> LoadIndex(long offset, long length, UInt32 magic) {
            //
            // Tablet index:
            //   [ magic (4 bytes) ]
            //   [ index record 1  ]
            //   ...
            //   [ index record N  ]
            //
            stream.Seek(offset, SeekOrigin.Begin);
            var m = stream.ReadInt();
            if (m != magic) {
                var msg = String.Format("Bad index magic {0:X}, expected {1:X}", m, magic);
                throw new TabletValidationException(msg);
            }

            var ret = new List<TabletIndexRecord>();

            while (stream.Position < offset + length) {
                ret.Add(ReadIndexRecord());
            }

            return ret;
        }

        private TabletIndexRecord ReadIndexRecord() {
            //
            // Tablet index record:
            //   [ file offset (msgpack uint, uint64 max)  ]
            //   [ block length (msgpack uint, uint32 max) ]
            //   [ first-key/name (msgpack raw)            ]
            //
            var i = new TabletIndexRecord();
            i.Offset = Unpacking.UnpackObject(stream).AsInt64();
            i.Length = Unpacking.UnpackObject(stream).AsInt32();
            var dataLen = (int)Unpacking.UnpackByteStream(stream).Length;
            i.Data = new byte[dataLen];
            stream.Read(i.Data, 0, dataLen);
            return i;
        }

        internal TabletFooter LoadFooter() {
            //
            // Tablet footer (40 bytes):
            //   [ meta index offset (msgpack uint64) ] - 9 bytes
            //   [ meta index length (msgpack uint64) ] - 9 bytes
            //   [ data index offset (msgpack uint64) ] - 9 bytes
            //   [ data index length (msgpack uint64) ] - 9 bytes
            //   [ magic ] - 4 bytes
            //
            stream.Seek(-40, SeekOrigin.End);
            var footer = new TabletFooter();
            footer.MetaIndexOffset = Unpacking.UnpackObject(stream).AsInt64();
            footer.MetaIndexLength = Unpacking.UnpackObject(stream).AsInt64();
            footer.DataIndexOffset = Unpacking.UnpackObject(stream).AsInt64();
            footer.DataIndexLength = Unpacking.UnpackObject(stream).AsInt64();
            var magic = stream.ReadInt();
            if (magic != Constants.TabletMagic) {
                var msg = String.Format("Bad tablet magic {0:X}", magic);
                throw new TabletValidationException(msg);
            }
            return footer;
        }

        internal struct TabletFooter {
            public long MetaIndexOffset;
            public long MetaIndexLength;
            public long DataIndexOffset;
            public long DataIndexLength;
        }

        internal struct TabletIndexRecord {
            public long Offset;
            public int Length;
            public byte[] Data;
        }

        internal class TabletIndexRecordDataComparer : IComparer<TabletIndexRecord> {
            public int Compare(TabletIndexRecord x, TabletIndexRecord y) {
                return x.Data.CompareKey(y.Data);
            }
        }
    }

    public class TabletValidationException : Exception {
        public TabletValidationException(String msg) : base(msg) {}
    }

    internal class TabletWriter {
        private SnappyCompressor compressor;

        public TabletWriter() {
            compressor = new SnappyCompressor();
        }

        internal void WriteTabletHeader(BinaryWriter writer) {
            //
            // Tablet header:
            // [ magic (4-bytes) ] [ 0x01 ] [ 3 unused bytes ]
            //
            var buf = new byte[8];
            var magicBuf = Constants.TabletMagic.ToNetworkBytes();
            Buffer.BlockCopy(magicBuf, 0, buf, 0, magicBuf.Length);
            buf[4] = 0x01;
            writer.Write(buf);
        }

        private void FlushBlock(BinaryWriter writer, BlockWriter blockWriter, Packer indexPacker, byte type, bool compression) {
            var offset = writer.BaseStream.Position;
            var output = blockWriter.Finish();
            var buf = output.Buffer;

            if (compression) {
                int maxLen = compressor.MaxCompressedLength(buf.Length);
                var compressed = new byte[maxLen];
                var len = compressor.Compress(buf, 0, buf.Length, compressed);

                if (len < buf.Length) {
                    // Only compress if there's an advantage.
                    buf = compressed.Take(len).ToArray();
                    type |= 1;  // Set compressed field.
                }
            }

            // Write block packing info.
            var checksum = Crc32.ChecksumIeee(buf);
            var packer = Packer.Create(writer.BaseStream, false);
            packer.Pack((uint)checksum);
            packer.Pack((uint)type);
            packer.Pack((uint)buf.Length);

            // Write block.
            writer.Write(buf);

            var length = writer.BaseStream.Position - offset;
            indexPacker.Pack((UInt64)offset);
            indexPacker.Pack((UInt32)length);
            indexPacker.PackRaw(output.FirstKey);

            blockWriter.Reset();
        }

        private byte[] WriteBlocks(BinaryWriter writer, IEnumerable<IKeyValuePair> kvs, byte type, TabletWriterOptions opts) {
            var indexStream = new MemoryStream();
            var indexPacker = Packer.Create(indexStream);

            var blockWriter = new BlockWriter(opts.KeyRestartInterval);

            foreach (var p in kvs) {
                blockWriter.Append(p.Key, p.Value);
                if (blockWriter.Size >= opts.BlockSize) {
                    FlushBlock(writer, blockWriter, indexPacker, type, opts.BlockCompression);
                }
            }

            // Flush the rest.
            FlushBlock(writer, blockWriter, indexPacker, type, opts.BlockCompression);

            return indexStream.GetBuffer().Take((int)indexStream.Length).ToArray();
        }

        public void WriteTablet(BinaryWriter writer, IEnumerable<IKeyValuePair> kvs, TabletWriterOptions opts) {
            WriteTabletHeader(writer);

            // Write data blocks.
            var dataIndex = WriteBlocks(writer, kvs, (byte)0x00, opts);

            // Write meta blocks (not implemented).
            //var metaIndex = WriteBlocks(writer, metaKvs, (byte)0x02, opts);
            var metaIndex = new byte[0];

            // Pack meta index block.
            UInt64 metaIndexOffset = (UInt64)writer.BaseStream.Position;
            UInt64 metaIndexLength = (UInt64)(metaIndex.Length + 4);  // + 4 for magic number.
            writer.Write(Constants.MetaIndexMagic.ToNetworkBytes());
            writer.Write(metaIndex);

            // Pack data index block.
            UInt64 dataIndexOffset = (UInt64)writer.BaseStream.Position;
            UInt64 dataIndexLength = (UInt64)(dataIndex.Length + 4);  // + 4 for magic number.
            writer.Write(Constants.DataIndexMagic.ToNetworkBytes());
            writer.Write(dataIndex);

            // Tablet footer.
            writer.Write(metaIndexOffset.ToMsgPackUInt64());
            writer.Write(metaIndexLength.ToMsgPackUInt64());
            writer.Write(dataIndexOffset.ToMsgPackUInt64());
            writer.Write(dataIndexLength.ToMsgPackUInt64());
            writer.Write(Constants.TabletMagic.ToNetworkBytes());
        }
    }

    internal struct TabletWriterOptions {
        public UInt32 BlockSize { get; set; }
        public bool BlockCompression { get; set; }
        public int KeyRestartInterval { get; set; }
    }
}
