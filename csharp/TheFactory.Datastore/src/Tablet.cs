using System;
using System.IO;
using System.Collections.Generic;
using MsgPack;
using Snappy.Sharp;

namespace TheFactory.Datastore {
    public class Tablet {
        private Stream stream;
        private SnappyDecompressor decompressor;

        public Tablet(Stream stream) {
            this.stream = stream;
            decompressor = new SnappyDecompressor();
        }

        public IEnumerable<Block.BlockPair> Find() {
            return Find(null);
        }

        public IEnumerable<Block.BlockPair> Find(byte[] term) {
            if (term == null || term.Length == 0) {
                // Find with no term; start at the beginning.
                var block = LoadBlock(0);
                return block.Find(null);
            }

            // TODO: find the correct block in the index given a search term.
            throw new NotImplementedException();
        }

        private Block LoadBlock(long offset) {
            //
            // In a tablet, a block is preceeded by:
            // [ checksum (msgpack uint32)          ]
            // [ type/compression (msgpack fixpos)  ]
            // [ length (msgpack uint32)            ]
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
    }

    public class TabletValidationException : Exception {
        public TabletValidationException(String msg) : base(msg) {}
    }
}
