using System;
using Snappy.Sharp;
using System.IO;
using MsgPack;
using System.Collections.Generic;
using TheFactory.Datastore.Helpers;
using System.Linq;

namespace TheFactory.Datastore {

    public class TabletWriterOptions {
        public UInt32 BlockSize { get; set; }
        public bool BlockCompression { get; set; }
        public int KeyRestartInterval { get; set; }

        public TabletWriterOptions() {
            BlockSize = 32768;
            BlockCompression = true;
            KeyRestartInterval = 128;
        }
    }

    internal class TabletWriter {
        // Use a static SnappyCompressor to minimize log chatter.
        static SnappyCompressor compressor = new SnappyCompressor();

        public TabletWriter() {
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
            if (output.FirstKey == null) {
                // Flushing an empty block: don't bother with the rest, but reset blockWriter
                // to ensure a clean slate.
                blockWriter.Reset();
                return;
            }

            var buf = output.Buffer;

            if (compression) {
                int maxLen = compressor.MaxCompressedLength(buf.Length);
                var compressed = new byte[maxLen];
                var len = compressor.Compress(buf.Array, buf.Offset, buf.Length, compressed);

                if (len < buf.Length) {
                    // Only compress if there's an advantage.
                    buf = new Slice(compressed, 0, len);
                    type |= 1;  // Set compressed field.
                }
            }

            // Write block packing info.
            var checksum = Crc32.ChecksumIeee(buf.Array, buf.Offset, buf.Length);
            var packer = Packer.Create(writer.BaseStream, false);
            packer.Pack((uint)checksum);
            packer.Pack((uint)type);
            packer.Pack((uint)buf.Length);

            // Write block.
            writer.Write(buf.Array, buf.Offset, buf.Length);

            var length = writer.BaseStream.Position - offset;
            indexPacker.Pack((UInt64)offset);
            indexPacker.Pack((UInt32)length);
            indexPacker.PackRaw((byte[])output.FirstKey);

            blockWriter.Reset();
        }

        private byte[] WriteBlocks(BinaryWriter writer, IEnumerable<IKeyValuePair> kvs, byte type, TabletWriterOptions opts) {
            var indexStream = new MemoryStream();
            var indexPacker = Packer.Create(indexStream);

            var blockWriter = new BlockWriter(opts.KeyRestartInterval);

            foreach (var p in kvs) {
                blockWriter.Append(p);
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

}

