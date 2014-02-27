using System;
using System.IO;
using System.Collections.Generic;

namespace TheFactory.Datastore {

    // This class represents an atomic write batch for the datastore: it
    // collects Put() and Delete() operations in one place before they
    // can be applied en masse. The order of Put/Delete operations is
    // preserved.
    //
    // The format of each Put pair is:
    //    <msgpack raw key> | <msgpack raw value>
    // and for a Delete:
    //    <msgpack raw key> | <msgpack nil>
    //
    // These structures are appended to the buffer in order, so its contents will be:
    //    <key1> <val1> <key2> <val2> ...
    //
    // This can later be extended with a per-batch sequence number if we need to provide
    // datastore snapshots.
    public class Batch {
        MemoryStream stream;

        public Batch() {
            stream = new MemoryStream();
        }

        public Batch(Slice buffer) {
            stream = new MemoryStream(buffer.Array, buffer.Offset, buffer.Length, false);
        }

        public void Put(Slice key, Slice val) {
            WriteRaw(key);
            WriteRaw(val);
        }

        public void Delete(Slice key) {
            WriteRaw(key);
            stream.WriteByte((byte)MiniMsgpackCode.NilValue);
        }

        public void Clear() {
            stream.Position = 0;
            stream.SetLength(0);
        }

        public bool IsEmpty() {
            return stream.Length == 0;
        }

        public Slice ToSlice() {
            return new Slice(stream.ToArray(), 0, (int)stream.Length);
        }

        private void WriteRaw(Slice raw) {
            // pack length and content separately so we can write without an intermediate copy
            MiniMsgpack.WriteRawLength(stream, raw.Length);
            stream.Write(raw.Array, raw.Offset, raw.Length);
        }

        private Slice ReadRaw(int flag, Stream reader, byte[] buffer) {
            // reader must be a MemoryStream over buffer. This routine reads the
            // next raw value from reader, but returning it as a Slice over buffer
            int len = MiniMsgpack.UnpackRawLength(flag, reader);
            Slice ret = new Slice(buffer, (int)reader.Position, len);

            reader.Position += len;
            return ret;
        }

        public IEnumerable<IKeyValuePair> Pairs() {
            // get the underlying buffer and create a new reader, so we can return
            // slices from the existing data rather than copying
            byte[] buf = stream.ToArray();
            Stream reader = new MemoryStream(buf, 0, (int)stream.Length);

            var pair = new Pair();
            while (reader.Position < reader.Length) {
                pair.Reset();

                int flag = reader.ReadByte();
                pair.Key = ReadRaw(flag, reader, buf);

                flag = reader.ReadByte();
                if (flag == MiniMsgpackCode.NilValue) {
                    pair.IsDeleted = true;
                } else {
                    pair.Value = ReadRaw(flag, reader, buf);
                }

                yield return pair;
            }

            yield break;
        }
    }
}

