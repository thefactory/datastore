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
            packer.PackRaw(key.Skip(prefix).ToArray());
            packer.PackRaw(val);

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
            public byte[] FirstKey { get; private set; }
            public Slice Buffer { get; private set; }

            public BlockWriterOutput(byte[] firstKey, Slice buf) {
                FirstKey = firstKey;
                Buffer = buf;
            }
        }
    }

    internal class Block {
        private Stream stream;
        private long start, length;
        private BlockPair pair;

        public Block(Slice block) {
            this.stream = new MemoryStream(block.Array, block.Offset, block.Length);
            this.start = 0;
            this.length = block.Length;
            this.pair = new BlockPair(stream);
        }

        // We end up using this quite a lot.
        private int? numRestarts;
        private int NumRestarts() {
            if (numRestarts != null) {
                return (int)numRestarts;
            }
            var end = start + length - 4;
            stream.Seek(end, SeekOrigin.Begin);
            numRestarts = (int)stream.ReadInt();
            return (int)numRestarts;
        }

        private IKeyValuePair ReadNext() {
            var prefix = Unpacking.UnpackObject(stream).AsInt32();
            var suffix = (int)Unpacking.UnpackByteStream(stream).Length;
            var key = new byte[prefix + suffix];
            if (prefix > 0) {
                Buffer.BlockCopy(pair.Key, 0, key, 0, prefix);
            }
            stream.Read(key, prefix, suffix);
            pair.Key = (Slice)key;
            pair.ValueLength = (int)Unpacking.UnpackByteStream(stream).Length;
            pair.ValueOffset = stream.Position;
            stream.Seek(pair.ValueLength, SeekOrigin.Current);  // cue.
            return pair;
        }

        private IEnumerable<IKeyValuePair> Pairs(long from) {
            var end = start + length - 4 - (4 * NumRestarts());
            stream.Seek(from, SeekOrigin.Begin);  // rew.

            while (stream.Position < end) {
                yield return ReadNext();
            }

            yield break;
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find(null);
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            if (term == null || term.Length == 0) {
                return Pairs(start);
            }

            long candidate = start;

            var restarts = start + length - 4 - (4 * NumRestarts());
            var comparer = new KeyComparer(term, stream, start, restarts);

            if (NumRestarts() > 0) {
                // We really just care about how many restarts we have as
                // array values. Generate an artificial array of indexes.
                var a = Enumerable.Range(0, NumRestarts()).ToArray();

                // -1 represents the search term passed into the KeyComparer.
                var index = Array.BinarySearch(a, -1, comparer);
                if (index < 0) {
                    // If we're less than all values, we'll get ~0, so index
                    // should be 0. All other cases should result in ~index - 1.
                    var i = ~index;
                    index = i == 0 ? 0 : i - 1;
                }

                stream.Seek(restarts + (index * 4), SeekOrigin.Begin);
                candidate = (int)stream.ReadInt();
            }

            foreach (var p in Pairs(candidate)) {
                if (Slice.Compare(p.Key, term) >= 0) {
                    // once p >= term, break
                    break;
                }
                candidate = stream.Position;
            }

            // return pairs from here until the end.
            return Pairs(candidate);
        }

        //
        // KeyComparer's Compare method actually deals with the translation
        // from a restart index's offset (in known block of bytes in a stream)
        // to a byte-array representing a key which is compared against a
        // search term passed into its constructor.
        //
        // It tends to look like we're comparing a list of ints from 0 to the
        // number of restart indexes when we're actually comparing byte-arrays
        // representing keys.
        //

        public class KeyComparer : IComparer<int> {
            private byte[] term;
            private Stream stream;
            private long start, restarts;

            public KeyComparer(byte[] term, Stream stream, long start, long restarts) {
                this.term = term;
                this.stream = stream;
                this.start = start;
                this.restarts = restarts;
            }

            private long ReadRestartOffset(int index) {
                stream.Seek(restarts + (index * 4), SeekOrigin.Begin);
                return (long)stream.ReadInt();
            }

            private byte[] ReadKey(long offset) {
                // Ignore prefix -- restart means it's 0 (one-byte fixpos).
                stream.Seek(start + offset + 1, SeekOrigin.Begin);
                var length = (int)Unpacking.UnpackByteStream(stream).Length;
                var key = new byte[length];
                stream.Read(key, 0, length);
                return key;
            }

            private byte[] GetKey(int index) {
                var offset = ReadRestartOffset(index);
                return ReadKey(offset);
            }

            public int Compare(int x, int y) {
                byte[] xVal, yVal;

                // x or y can be negative (representing a search term passed
                // into the ctor).
                xVal = x < 0 ? term : GetKey(x);
                yVal = y < 0 ? term : GetKey(y);

                return xVal.CompareKey(yVal);
            }
        }

        private class BlockPair : IKeyValuePair {
            private Stream stream;
            public long ValueOffset;
            public int ValueLength;

            public bool IsDeleted { get; set; }
            public Slice Key { get; set; }

            public Slice Value {
                get {
                    var pos = stream.Position;  // stash position.
                    var val = new byte[ValueLength];

                    stream.Seek(ValueOffset, SeekOrigin.Begin);
                    var read = stream.Read(val, 0, ValueLength);

                    stream.Seek(pos, SeekOrigin.Begin);  // restore position;
                    if (read < ValueLength) {
                        throw new InvalidOperationException("Not enough bytes");
                    }

                    return (Slice)val;
                }
            }

            public BlockPair(Stream stream) {
                this.stream = stream;
            }
        }
    }
}
