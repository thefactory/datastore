using System;
using System.IO;
using System.Collections.Generic;

namespace TheFactory.Datastore {

    public class StreamTablet {
        Stream stream;
        TabletReaderOptions opts;
        TabletReader reader;

        public StreamTablet(Stream stream) : this(stream, new TabletReaderOptions()) {
        }

        public StreamTablet(Stream stream, TabletReaderOptions opts) {
            this.stream = stream;
            this.opts = opts;
            this.reader = new TabletReader();
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            byte[] buf = new byte[Constants.TabletHeaderLength];
            if (FillBuffer(stream, buf) != Constants.TabletHeaderLength) {
                throw new TabletValidationException("stream too short: no tablet header");
            }

            // Consume and validate the Tablet header.
            reader.ParseHeader((Slice)buf);

            while (true) {
                // This is a little hackish: we need to peek 4 bytes to see if we've
                // reached the end of the data blocks.
                if (PeekMagic(stream) == Constants.MetaIndexMagic) {
                    break;
                }

                TabletBlock block = new TabletBlock(NextBlock(stream));
                if (opts.VerifyChecksums && !block.IsChecksumValid) {
                    new TabletValidationException("bad block checksum");
                }

                BlockReader br = new BlockReader(block.KvData);

                foreach (var kv in br.Find(term)) {
                    yield return kv;
                }
            }
        }

        int FillBuffer(Stream stream, byte[] buf) {
            int read = 0;
            while (read < buf.Length) {
                int part = stream.Read(buf, read, buf.Length - read);
                if (part == 0) {
                    // End of stream reached.
                    break;
                }

                read += part;
            }

            return read;
        }

        uint PeekMagic(Stream stream) {
            byte[] buf = new byte[4];
            var pos = stream.Position;

            try {
                if (FillBuffer(stream, buf) != buf.Length) {
                    throw new TabletValidationException("stream too short: expected block header");
                }

                return Utils.ToUInt32((Slice)buf);
            } finally {
                stream.Position = pos;
            }
        }

        Slice NextBlock(Stream stream) {
            var start = stream.Position;
            var dataLen = reader.ReadBlockHeaderLength(stream);

            // Ugh. Use stream.Position to decide how long the header itself was, then
            // add in the length of data it contains.
            var blockLen = (stream.Position - start) + dataLen;

            stream.Position = start;
            byte[] buf = new byte[blockLen];
            if (FillBuffer(stream, buf) != buf.Length) {
                throw new TabletValidationException("stream too short: unfinished block");
            }

            return (Slice)buf;
        }
    }
}

