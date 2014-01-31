using System;
using System.IO;
using System.Collections.Generic;
using MsgPack;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {

    public class TabletReaderOptions {
        public bool VerifyChecksums { get; set; }

        public TabletReaderOptions() {
            VerifyChecksums = false;
        }
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

        internal TabletFooter ParseFooter(Slice buf) {
            //
            // Tablet footer (40 bytes):
            //   [ meta index offset (msgpack uint) ]
            //   [ meta index length (msgpack uint) ]
            //   [ data index offset (msgpack uint) ]
            //   [ data index length (msgpack uint) ]
            //   [ padding to 40 bytes total (magic included) ]
            //   [ magic ] - 4 bytes
            //

            if (buf.Length != 40) {
                var msg = String.Format("Internal error: tablet footer length != 40 (was {0})", buf.Length);
                throw new TabletValidationException(msg);
            }

            var magic = Utils.ToUInt32(buf.Subslice(-4));
            if (magic != Constants.TabletMagic) {
                var msg = String.Format("Bad tablet magic {0:X}", magic);
                throw new TabletValidationException(msg);
            }

            var stream = buf.ToStream();

            var footer = new TabletFooter();
            footer.MetaIndexOffset = Unpacking.UnpackObject(stream).AsInt64();
            footer.MetaIndexLength = Unpacking.UnpackObject(stream).AsInt64();
            footer.DataIndexOffset = Unpacking.UnpackObject(stream).AsInt64();
            footer.DataIndexLength = Unpacking.UnpackObject(stream).AsInt64();
            return footer;
        }
    }
}

