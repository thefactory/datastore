using System;
using System.IO;
using MsgPack;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {

    internal class BlockWriter {
        private MemoryStream body, footer;
        private Packer packer;
        private Slice previousKey, firstKey;
        private int keyRestartInterval, keyRestartCount;

        Slice tombstone;

        public BlockWriter(int keyRestartInterval) {
            body = new MemoryStream();
            packer = Packer.Create(body);
            footer = new MemoryStream();
            this.keyRestartInterval = keyRestartInterval;
            keyRestartCount = 0;

            tombstone = (Slice)(new byte[1]{MiniMsgpackCode.NilValue});
        }

        public UInt32 Size {
            get {
                return (UInt32)(body.Length + footer.Length + 4);
            }
        }

        public void Append(IKeyValuePair kv) {
            if (kv.IsDeleted) {
                Append(kv.Key, tombstone);
            } else {
                Append(kv.Key, kv.Value);
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
}

