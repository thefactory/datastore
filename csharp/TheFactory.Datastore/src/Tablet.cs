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

        IEnumerable<IKeyValuePair> Find(Slice term);

        string Filename { get; }
    }

    internal class MemoryTablet : ITablet {
        private SortedSet<IKeyValuePair> backing;
        private ReaderWriterLockSlim backingLock;

        public string Filename {
            get {
                return null;
            }
        }

        // Deleted key marker -- consumers of Get() and Find() enumerator
        // should check against this reference and take appropriate action.
        public static Slice Tombstone = (Slice)(new byte[] {0x74, 0x6f, 0x6d, 0x62});

        public MemoryTablet() {
            var cmp = new MemoryTabletKeyComparer();
            backing = new SortedSet<IKeyValuePair>(cmp);
            backingLock = new ReaderWriterLockSlim();
        }

        public void Apply(Batch batch) {
            foreach (IKeyValuePair kv in batch.Pairs()) {
                if (kv.IsDeleted) {
                    Delete(kv.Key.Detach());
                } else {
                    Set(kv.Key.Detach(), kv.Value.Detach());
                }
            }
        }

        public void Set(Slice key, Slice val) {
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

        public void Delete(Slice key) {
            Set(key, Tombstone);
        }

        public void Close() {
            backing.Clear();
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find((Slice)null);
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            if (backing.Count == 0) {
                yield break;
            }

            backingLock.EnterReadLock();
            try {
                var set = backing;

                if (term != null) {
                    var termSlice = (Slice)term;
                    if (Slice.Compare(backing.Max.Key, termSlice) < 0) {
                        // Max key is less than the search term -- empty.
                        yield break;
                    }
                    var searchTerm = new MemoryKeyValuePair(termSlice, null);
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
            public Slice Key { get; private set; }
            public Slice Value { get; private set; }
            public bool IsDeleted {
                get {
                    return ReferenceEquals(Value, Tombstone);
                }
            }

            public MemoryKeyValuePair(Slice key, Slice val) {
                Key = key;
                Value = val;
            }
        }

        private class MemoryTabletKeyComparer : IComparer<IKeyValuePair> {
            public int Compare(IKeyValuePair x, IKeyValuePair y) {
                return Slice.Compare(x.Key, y.Key);
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
        private TabletReader reader;
        private List<TabletIndexRecord> dataIndex, metaIndex;

        public string Filename { get; private set; }

        public FileTablet(string filename) : this(new FileStream(filename, FileMode.Open, FileAccess.Read)) {
            Filename = filename;
        }

        internal FileTablet(Stream stream) {
            this.stream = stream;
            reader = new TabletReader();
        }

        public void Close() {
            stream.Close();
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find(null);
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
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
                Block block;
                try {
                    block = LoadBlock(dataIndex[i].Offset);
                } catch(TabletValidationException) {
                    // Bad block checksum.
                    continue;
                }

                // search for term in the first block, but yield all kv pairs from the rest
                Slice blockTerm = (i == blockIndex) ? term : null;

                foreach (var p in block.Find(blockTerm)) {
                    yield return p;
                }
            }

            yield break;
        }

        internal Block LoadBlock(long offset) {
            stream.Seek(offset, SeekOrigin.Begin);
            var blockData = reader.ReadBlock(stream);

            if (blockData.Info.Checksum != 0 && blockData.Info.Checksum != blockData.Checksum) {
                throw new TabletValidationException("bad block checksum");
            }

            return new Block((Slice)blockData.Data);
        }

        internal List<TabletIndexRecord> LoadIndex(long offset, long length, UInt32 magic) {
            stream.Seek(offset, SeekOrigin.Begin);
            return reader.ReadIndex(stream, length, magic);
        }

        internal TabletFooter LoadFooter() {
            stream.Seek(-40, SeekOrigin.End);
            return reader.ReadFooter(stream);
        }

        internal class TabletIndexRecordDataComparer : IComparer<TabletIndexRecord> {
            public int Compare(TabletIndexRecord x, TabletIndexRecord y) {
                return Slice.Compare(x.Data, y.Data);
            }
        }
    }

    public class TabletValidationException : Exception {
        public TabletValidationException(String msg) : base(msg) {}
    }

    internal enum BlockType {
        Data,
        Meta
    }

    internal struct TabletHeader {
        public byte[] Raw { get; private set; }
        private MemoryStream s;

        private Stream RawStream {
            get {
                if (s == null) {
                    s = new MemoryStream(Raw);
                }
                return s;
            }
        }

        public UInt32 Magic {
            get {
                RawStream.Seek(0, SeekOrigin.Begin);
                return RawStream.ReadInt();
            }
        }

        public UInt32 Version {
            get {
                RawStream.Seek(4, SeekOrigin.Begin);
                var bytes = RawStream.ReadBytes(1);
                return bytes[0];
            }
        }

        public TabletHeader(byte[] raw) : this() {
            Raw = raw;
        }
    }

    internal struct TabletBlockInfo {
        private byte type;

        public UInt32 Checksum { get; private set; }
        public int Length { get; private set; }
        public byte[] Raw { get; private set; }

        //
        // Hide the bitfield representing type.
        // 0b000000TC
        // C: block compression: 0 = None, 1 = Snappy
        // T: block type: 0 = Data block, 1 = Metadata block
        //

        public BlockType Type {
            get {
                return ((int)type & (1 << 1)) == 0 ? BlockType.Data : BlockType.Meta;
            }
        }

        public bool IsCompressed {
            get {
                return ((int)type & 1) == 1;
            }
        }

        public TabletBlockInfo(UInt32 checksum, byte type, int length, byte[] raw) : this() {
            Checksum = checksum;
            this.type = type;
            Length = length;
            Raw = raw;
        }
    }

    internal struct TabletBlockData {
        public TabletBlockInfo Info { get; private set; }
        public byte[] Raw { get; private set; }
        public byte[] Data {
            get {
                if (Info.IsCompressed) {
                    var decompressor = new SnappyDecompressor();
                    return decompressor.Decompress(Raw, 0, Raw.Length);
                }
                return Raw;
            }
        }

        // Computed checksum (not necessarily the same as Info.Checksum).
        private UInt32 checksum;
        public UInt32 Checksum {
            get {
                return Crc32.ChecksumIeee(Raw);
            }
        }

        public TabletBlockData(TabletBlockInfo info, byte[] raw) : this() {
            Info = info;
            Raw = raw;
        }
    }

    internal struct TabletIndexRecord {
        public long Offset;
        public int Length;
        public Slice Data;
    }

    internal struct TabletFooter {
        public long MetaIndexOffset;
        public long MetaIndexLength;
        public long DataIndexOffset;
        public long DataIndexLength;
    }

    internal class TabletReader {
        public TabletReader() {
        }

        internal TabletHeader ReadHeader(Stream stream) {
            //
            // [ tablet magic ] - 4 bytes.
            // [ version      ] - 1 byte.
            // [ unused       ] - 3 bytes.
            //
            var buf = stream.ReadBytes(8);

            var header = new TabletHeader(buf);
            if (header.Magic != Constants.TabletMagic) {
                throw new TabletValidationException("bad magic");
            }

            if (header.Version < 1) {
                throw new TabletValidationException("bad version");
            }

            return header;
        }

        internal TabletBlockInfo ReadBlockInfo(Stream stream) {
            //
            // In a tablet, a block is preceeded by:
            //   [ checksum (msgpack uint32)          ]
            //   [ type/compression (msgpack fixpos)  ]
            //   [ length (msgpack uint32)            ]
            //
            var pos = stream.Position;

            var checksum = Unpacking.UnpackObject(stream).AsUInt32();
            var type = Unpacking.UnpackObject(stream).AsInt32();
            var length = Unpacking.UnpackObject(stream).AsInt32();

            // This is probably awful. Read it again to get raw.
            var infoLen = stream.Position - pos;
            stream.Seek(-infoLen, SeekOrigin.Current);
            var raw = stream.ReadBytes((int)infoLen);

            return new TabletBlockInfo(checksum, (byte)type, length, raw);
        }

        internal TabletBlockData ReadBlock(Stream stream) {
            var info = ReadBlockInfo(stream);
            var raw = stream.ReadBytes(info.Length);
            return new TabletBlockData(info, raw);
        }

        private TabletIndexRecord ReadIndexRecord(Stream stream) {
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
            i.Data = (Slice)stream.ReadBytes(dataLen);
            return i;
        }

        internal List<TabletIndexRecord> ReadIndex(Stream stream, long length, UInt32 magic) {
            //
            // Tablet index:
            //   [ magic (4 bytes) ]
            //   [ index record 1  ]
            //   ...
            //   [ index record N  ]
            //
            long offset = stream.Position;
            var m = stream.ReadInt();
            if (m != magic) {
                var msg = String.Format("Bad index magic {0:X}, expected {1:X}", m, magic);
                throw new TabletValidationException(msg);
            }

            var ret = new List<TabletIndexRecord>();

            while (stream.Position < offset + length) {
                ret.Add(ReadIndexRecord(stream));
            }

            return ret;
        }

        internal TabletFooter ReadFooter(Stream stream) {
            //
            // Tablet footer (40 bytes):
            //   [ meta index offset (msgpack uint64) ] - 9 bytes
            //   [ meta index length (msgpack uint64) ] - 9 bytes
            //   [ data index offset (msgpack uint64) ] - 9 bytes
            //   [ data index length (msgpack uint64) ] - 9 bytes
            //   [ magic ] - 4 bytes
            //
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
                    buf = new Slice(compressed, 0, len);
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
