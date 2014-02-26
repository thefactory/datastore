using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using MsgPack;
using TheFactory.Snappy;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {

    internal class Constants {
        public const UInt32 TabletMagic = 0x0b501e7e;
        public const UInt32 MetaIndexMagic = 0x0ea7da7a;
        public const UInt32 DataIndexMagic = 0xda7aba5e;

        public const int TabletHeaderLength = 8;
        public const int TabletFooterLength = 40;
    }

    internal class FileTablet : ITablet {
        private TabletReaderOptions opts;
        private Stream stream;
        private TabletReader reader;
        private List<TabletIndexRecord> dataIndex;

        public string Filename { get; private set; }

        public FileTablet(string filename, TabletReaderOptions opts) : this(new FileStream(filename, FileMode.Open, FileAccess.Read), opts) {
            Filename = filename;
        }

        internal FileTablet(Stream stream, TabletReaderOptions opts) {
            this.stream = stream;
            reader = new TabletReader();
            this.opts = opts;

            var footer = LoadFooter();
            dataIndex = LoadIndex(footer.DataIndexOffset, footer.DataIndexLength, Constants.DataIndexMagic);
        }

        public void Close() {
            stream.Close();
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find(null);
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
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
                    block = LoadBlock(dataIndex[i].Offset, dataIndex[i].Length);
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

        object posLock = new object();
        Slice PRead(long offset, long length) {
            lock (posLock) {
                stream.Seek(offset, SeekOrigin.Begin);
                return (Slice)stream.ReadBytes((int)length);
            }
        }

        internal BlockReader LoadBlock(long offset, long length) {
            var block = new TabletBlock(PRead(offset, length));

            if (opts.VerifyChecksums) {
                if (!block.IsChecksumValid) {
                    throw new TabletValidationException("bad block checksum");
                }
            }

            return new BlockReader(block.KvData);
        }

        internal List<TabletIndexRecord> LoadIndex(long offset, long length, UInt32 magic) {
            var slice = PRead(offset, length);
            return reader.ParseIndex(slice, magic);
        }

        internal TabletFooter LoadFooter() {
            var slice = PRead(stream.Length - Constants.TabletFooterLength, Constants.TabletFooterLength);
            return reader.ParseFooter(slice);
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
        public UInt32 Magic { get; private set; }
        public UInt32 Version { get; private set; }

        public TabletHeader(Slice data) : this() {
            Magic = Utils.ToUInt32(data);
            Version = data[4];
        }
    }

    internal struct TabletBlock {
        public UInt32 Checksum { get; private set; }
        public bool IsChecksumValid {
            get {
                // Checksum == 0 is considered valid for historical reasons.
                return Checksum == 0 ||
                    Checksum == Crc32.ChecksumIeee(rawData.Array, rawData.Offset, rawData.Length);
            }
        }

        private byte flags;
        public BlockType Type {
            get {
                return (flags & 0x2) == 0 ? BlockType.Data : BlockType.Meta;
            }
        }

        public bool IsCompressed { get { return (flags & 0x1) != 0; } }

        // rawData stores the possibly compressed subslice of the block that
        // contains its data; kvData is lazily loaded and contains the
        // iterable key-value pairs.
        private Slice rawData;
        private Slice kvData;

        public Slice KvData {
            get {
                if (kvData == null) {
                    lock (rawData) {
                        // To be fixed: this is an unnecessary copy of rawData. Needs
                        // new SnappyDecoder API.
                        kvData = (Slice)SnappyDecoder.Decode(rawData.ToArray());
                    }
                }

                return kvData;
            }
        }

        public TabletBlock(Slice block): this() {
            int kvOffset, kvLength;
            using (var stream = block.ToStream()) {
                Checksum = Unpacking.UnpackObject(stream).AsUInt32();
                flags = (byte)Unpacking.UnpackObject(stream).AsInt32();

                kvLength = Unpacking.UnpackObject(stream).AsInt32();
                kvOffset = (int)stream.Position;
            }

            // Verify that this slice is the correct length.
            if (kvOffset + kvLength != block.Length) {
                throw new TabletValidationException("block data doesn't match its header length");
            }

            if (IsCompressed) {
                // kvData will be loaded lazily from this
                rawData = block.Subslice(kvOffset, kvLength);
                kvData = null;
            } else {
                rawData = kvData = block.Subslice(kvOffset, kvLength);
            }
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
