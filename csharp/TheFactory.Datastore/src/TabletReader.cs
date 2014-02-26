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

        public override string ToString() {
            return String.Format("VerifyChecksums = {0}", VerifyChecksums);
        }
    }

    internal class TabletReader {
        public TabletReader() {
        }

        internal TabletHeader ParseHeader(Slice slice) {
            //
            // [ tablet magic ] - 4 bytes.
            // [ version      ] - 1 byte.
            // [ unused       ] - 3 bytes.
            //

            var header = new TabletHeader(slice);
            if (header.Magic != Constants.TabletMagic) {
                throw new TabletValidationException(String.Format("bad magic: was {0:X8} expected {0:X8}", header.Magic, Constants.TabletMagic));
            }

            if (header.Version < 1) {
                throw new TabletValidationException("bad version");
            }

            return header;
        }

        internal int ReadBlockHeaderLength(Stream stream) {
            // Read a block header long enough to get its length; the caller
            // will maintain the stream position. This is a little hackish
            // but is necessary for our current stream API.

            Unpacking.UnpackObject(stream).AsUInt32();       // checksum
            Unpacking.UnpackObject(stream).AsInt32();        // flags
            return Unpacking.UnpackObject(stream).AsInt32(); // length
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

        internal List<TabletIndexRecord> ParseIndex(Slice slice, UInt32 magic) {
            //
            // Tablet index:
            //   [ magic (4 bytes) ]
            //   [ index record 1  ]
            //   ...
            //   [ index record N  ]
            //
            var m = Utils.ToUInt32(slice);
            if (m != magic) {
                var msg = String.Format("Bad index magic {0:X}, expected {1:X}", m, magic);
                throw new TabletValidationException(msg);
            }

            var recSlice = slice.Subslice(4);
            var stream = recSlice.ToStream();

            var ret = new List<TabletIndexRecord>();
            while (stream.Position < recSlice.Length) {
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

            if (buf.Length != Constants.TabletFooterLength) {
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

