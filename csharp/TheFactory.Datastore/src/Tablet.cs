using System;
using System.Collections.Generic;
using System.IO;
using MsgPack;
using Snappy.Sharp;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {
    public class Tablet {
        const UInt32 TabletMagic = 0x0b501e7e;
        const UInt32 MetaIndexMagic = 0x0ea7da7a;
        const UInt32 DataIndexMagic = 0xda7aba5e;

        private Stream stream;
        private SnappyDecompressor decompressor;
        private List<TabletIndexRecord> dataIndex, metaIndex;

        public Tablet(Stream stream) {
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
                metaIndex = LoadIndex(footer.MetaIndexOffset, footer.MetaIndexLength, MetaIndexMagic);
                dataIndex = LoadIndex(footer.DataIndexOffset, footer.DataIndexLength, DataIndexMagic);
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
            var checksum = Unpacking.UnpackObject(stream).AsInt32();
            var type = Unpacking.UnpackObject(stream).AsInt32();
            var length = Unpacking.UnpackObject(stream).AsInt32();

            // Read (maybe compressed) block-data into memory.
            var buf = new byte[length];
            stream.Read(buf, 0, length);

            byte[] data;

            switch (type) {
                case 0:  // uncompressed.
                    data = buf;
                    break;
                case 1:  // snappy.
                    // XXX: SnappyDecompressor doesn't operate on a stream,
                    // so we're possibly doubling up on memory here.
                    data = decompressor.Decompress(buf, 0, buf.Length);
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

        internal struct TabletIndexRecord {
            public long Offset;
            public int Length;
            public byte[] Data;
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
            if (magic != TabletMagic) {
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

        internal class TabletIndexRecordDataComparer : IComparer<TabletIndexRecord> {
            public int Compare(TabletIndexRecord x, TabletIndexRecord y) {
                return x.Data.CompareKey(y.Data);
            }
        }
    }

    public class TabletValidationException : Exception {
        public TabletValidationException(String msg) : base(msg) {}
    }
}
