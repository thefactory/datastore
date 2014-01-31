using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using MsgPack;
using Snappy.Sharp;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {

    internal class Constants {
        public const UInt32 TabletMagic = 0x0b501e7e;
        public const UInt32 MetaIndexMagic = 0x0ea7da7a;
        public const UInt32 DataIndexMagic = 0xda7aba5e;
    }

    internal class FileTablet : ITablet {
        private TabletReaderOptions opts;
        private Stream stream;
        private TabletReader reader;
        private List<TabletIndexRecord> dataIndex, metaIndex;

        public string Filename { get; private set; }

        public FileTablet(string filename, TabletReaderOptions opts) : this(new FileStream(filename, FileMode.Open, FileAccess.Read), opts) {
            Filename = filename;
        }

        internal FileTablet(Stream stream, TabletReaderOptions opts) {
            this.stream = stream;
            reader = new TabletReader();
            this.opts = opts;
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
                BlockReader block;
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

        internal BlockReader LoadBlock(long offset) {
            stream.Seek(offset, SeekOrigin.Begin);
            var blockData = reader.ReadBlock(stream);

            if (opts.VerifyChecksums) {
                if (blockData.Info.Checksum != 0 && blockData.Info.Checksum != blockData.Checksum) {
                    throw new TabletValidationException("bad block checksum");
                }
            }

            return new BlockReader((Slice)blockData.Data);
        }

        internal List<TabletIndexRecord> LoadIndex(long offset, long length, UInt32 magic) {
            stream.Seek(offset, SeekOrigin.Begin);
            return reader.ReadIndex(stream, length, magic);
        }

        internal TabletFooter LoadFooter() {
            stream.Seek(-40, SeekOrigin.End);
            return reader.ParseFooter((Slice)(stream.ReadBytes(40)));
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
}
