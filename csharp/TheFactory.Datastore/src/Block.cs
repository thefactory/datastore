using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using MsgPack;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {
    internal class BlockWriter {
        private MemoryStream body, footer;
        private Packer packer;
        private Slice previousKey, firstKey;
        private int keyRestartInterval, keyRestartCount;

        public BlockWriter(int keyRestartInterval) {
            body = new MemoryStream();
            packer = Packer.Create(body);
            footer = new MemoryStream();
            this.keyRestartInterval = keyRestartInterval;
            keyRestartCount = 0;
        }

        public UInt32 Size {
            get {
                return (UInt32)(body.Length + footer.Length + 4);
            }
        }

        public void Append(Slice key, Slice val) {
            if (firstKey == null) {
                firstKey = key.Detach();
            }

            var prefix = 0;
            if (keyRestartCount % keyRestartInterval != 0) {
                prefix = key.CommonBytes(previousKey);
            } else {
                footer.WriteInt((UInt32)body.Position);
            }

            //
            // [ key prefix length (msgpack uint) ]
            // [ rest of the key (msgpack raw)    ]
            // [ value (msgpack raw)              ]
            //
            packer.Pack(prefix);
            packer.PackRaw((byte[])key.Subslice(prefix));
            packer.PackRaw((byte[])val);

            previousKey = key.Detach();
            keyRestartCount = (keyRestartCount + 1) % keyRestartInterval;
        }

        public BlockWriterOutput Finish() {
            var numRestarts = footer.Length / 4;
            footer.WriteTo(body);
            body.WriteInt((UInt32)numRestarts);
            return new BlockWriterOutput(firstKey, new Slice(body.GetBuffer(), 0, (int)body.Length));
        }

        public void Reset() {
            body.SetLength(0);
            footer.SetLength(0);
            keyRestartCount = 0;
            firstKey = null;
            previousKey = null;
        }

        internal class BlockWriterOutput {
            public Slice FirstKey { get; private set; }
            public Slice Buffer { get; private set; }

            public BlockWriterOutput(Slice firstKey, Slice buf) {
                FirstKey = firstKey;
                Buffer = buf;
            }
        }
    }

    internal class Block {
        private Slice block;
        private Slice kvs;
        private int numRestarts;

        public Block(Slice block) {
            this.block = block;
            this.numRestarts = Utils.ToUInt32(block.Subslice(-4));

            // create a subslice that contains only the data in the block
            int end = block.Length - 4 * this.numRestarts - 4;
            this.kvs = block.Subslice(0, end);
        }

        private IEnumerable<Pair> Pairs(Slice slice, Slice skipTo) {
            // enumerate the key-value pairs in slice, optionally skipping to the key skipTo
            Stream stream = slice.ToStream();
            Pair pair = new Pair();

            Slice prevKey = null;
            while (stream.Position < stream.Length) {
                pair.Reset();

                int common = (int)MiniMsgpack.UnpackUInt32(stream);

                int suffixLength = MiniMsgpack.UnpackRawLength(stream.ReadByte(), stream);
                Slice suffix = slice.Subslice((int)stream.Position, suffixLength);
                stream.Position += suffixLength;

                /* combine the previous key and the new suffix to make the current key */
                if (common == 0) {
                    pair.Key = suffix;
                } else {
                    byte[] tmp = new byte[common + suffix.Length];
                    Buffer.BlockCopy(prevKey.Array, prevKey.Offset, tmp, 0, common);
                    Buffer.BlockCopy(suffix.Array, suffix.Offset, tmp, common, suffix.Length);
                    pair.Key = (Slice)tmp;
                }
                prevKey = pair.Key;

                var flag = stream.ReadByte();
                if (flag == MiniMsgpackCode.NilValue) {
                    pair.IsDeleted = true;
                } else {
                    int valueLength = MiniMsgpack.UnpackRawLength(flag, stream);
                    pair.Value = slice.Subslice((int)stream.Position, valueLength);
                    stream.Position += valueLength;
                }

                if (skipTo != null && (Slice.Compare(pair.Key, skipTo) < 0)) {
                    continue;
                } else {
                    // set skipTo == null once it's found so we don't check anymore
                    skipTo = null;
                }

                yield return pair;
            }

            yield break;
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find(null);
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            if (term == null || term.Length == 0 || numRestarts <= 1) {
                return Pairs(kvs, term);
            }

            // binary search the restarts to find the first one greater than term
            int restart = Utils.Search(numRestarts, (i) => {
                return Slice.Compare(RestartKey(i), term) > 0;
            });

            if (restart == 0) {
                // even the first restart key was greater than our term; return them all
                return Pairs(kvs, term);
            }

            // start from the previous restart and advance to term
            return Pairs(kvs.Subslice(RestartValue(restart - 1)), term);
        }

        Slice RestartKey(int n) {
            int pos = RestartValue(n);

            // skip the first byte at pos, which is guaranteed to be 0x0 because this is a restart
            Slice slice = kvs.Subslice(pos+1);
            Stream stream = slice.ToStream();

            int keyLength = MiniMsgpack.UnpackRawLength(stream.ReadByte(), stream);
            return slice.Subslice((int)stream.Position, keyLength);
        }

        int RestartValue(int n) {
            // decode the n'th restart to its position in the kv data
            return Utils.ToUInt32(block.Subslice(RestartPosition(n)));
        }

        int RestartPosition(int n) {
            if (n >= numRestarts) {
                throw new ArgumentOutOfRangeException("Invalid restart: " + n);
            }

            // return the in-block position of the n'th restart
            int indexStart = block.Length - (4 * numRestarts + 4);
            return indexStart + 4 * n;
        }
    }
}
