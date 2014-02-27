using System;
using TheFactory.Snappy;
using System.IO;
using System.Collections.Generic;
using TheFactory.Datastore.Helpers;
using System.Linq;
using Splat;

namespace TheFactory.Datastore {

    public class TabletWriterOptions {
        public UInt32 BlockSize { get; set; }
        public bool BlockCompression { get; set; }
        public int KeyRestartInterval { get; set; }

        // Temporary flag while we test TheFactory.Snappy: this ensures that all
        // compressed blocks can be round-tripped through Snappy.
        public bool VerifyBlockCompression { get; set; }

        public TabletWriterOptions() {
            BlockSize = 32768;
            BlockCompression = true;
            KeyRestartInterval = 128;
            VerifyBlockCompression = true;
        }

        public override string ToString() {
            return String.Format(
                "BlockSize = {0}\n" +
                "BlockCompression = {1}\n" +
                "KeyRestartInterval = {2}\n" +
                "VerifyBlockCompression = {3}",
                BlockSize, BlockCompression, KeyRestartInterval, VerifyBlockCompression);
        }
    }

    internal class TabletWriter : IEnableLogger {
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

        private void FlushBlock(BinaryWriter writer, BlockWriter blockWriter, Stream index, byte type, TabletWriterOptions opts) {
            var offset = writer.BaseStream.Position;
            var output = blockWriter.Finish();
            if (output.FirstKey == null) {
                // Flushing an empty block: don't bother with the rest, but reset blockWriter
                // to ensure a clean slate.
                blockWriter.Reset();
                return;
            }

            var buf = output.Buffer;

            if (opts.BlockCompression) {
                var comp = SnappyEncoder.Encode(buf.ToArray());
                var valid = true;

                // This VerifyBlockCompression block is temporary while we test TheFactory.Snappy.
                if (opts.VerifyBlockCompression) {
                    try {
                        var decomp = SnappyDecoder.Decode(comp);
                        if (decomp.CompareKey(buf.ToArray()) != 0) {
                            this.Log().Error("Block compression roundtrip failure: {0}", buf);
                            valid = false;
                        }
                    } catch (Exception e) {
                        this.Log().ErrorException(String.Format("Block compression roundtrip failure: {0}", buf), e);
                        valid = false;
                    }
                }

                if (valid && comp.Length < buf.Length) {
                    // Only compress if there's an advantage.
                    buf = (Slice)comp;
                    type |= 1;  // Set compressed field.
                }
            }

            // Write block packing info.
            var checksum = Crc32.ChecksumIeee(buf.Array, buf.Offset, buf.Length);
            MiniMsgpack.PackUInt(writer.BaseStream, checksum);
            MiniMsgpack.PackUInt(writer.BaseStream, type);
            MiniMsgpack.PackUInt(writer.BaseStream, (ulong)buf.Length);

            // Write block.
            writer.Write(buf.Array, buf.Offset, buf.Length);

            var length = writer.BaseStream.Position - offset;
            MiniMsgpack.PackUInt(index, (ulong)offset);
            MiniMsgpack.PackUInt(index, (ulong)length);
            MiniMsgpack.PackRaw(index, output.FirstKey);

            blockWriter.Reset();
        }

        private byte[] WriteBlocks(BinaryWriter writer, IEnumerable<IKeyValuePair> kvs, byte type, TabletWriterOptions opts) {
            var indexStream = new MemoryStream();
            var blockWriter = new BlockWriter(opts.KeyRestartInterval);

            foreach (var p in kvs) {
                blockWriter.Append(p);
                if (blockWriter.Size >= opts.BlockSize) {
                    FlushBlock(writer, blockWriter, indexStream, type, opts);
                }
            }

            // Flush the rest.
            FlushBlock(writer, blockWriter, indexStream, type, opts);

            return indexStream.ToArray();
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

