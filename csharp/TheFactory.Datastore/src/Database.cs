using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {
    public class Database {
        private List<ITablet> tablets;
        private List<ITablet> mutableTablets;
        public string Dir { get; private set; }

        internal Database() {
            tablets = new List<ITablet>();
            mutableTablets = new List<ITablet>();
            mutableTablets.Add(new MemoryTablet());
            // There's probably something that happens elsewhere which involves
            // replaying a durable log for an in-flight MemoryTablet.
        }

        public Database(string path) : this() {
            Dir = path;
            // Set up directory path.
        }

        public void Close() {
            while (tablets.Count > 0) {
                PopTablet();
            }
            // TODO: Not sure what to do with mutableTablets.
        }

        private bool EndOfBlocks(Stream stream) {
            var peek = stream.ReadInt();
            stream.Seek(-4, SeekOrigin.Current);
            // The meta index block signals the end of blocks.
            return peek == Constants.MetaIndexMagic;
        }

        public void PushTabletStream(Stream stream, Action<IEnumerable<IKeyValuePair>> callback) {
            PushTabletStream(stream, System.Guid.NewGuid().ToString(), callback);
        }

        internal void PushTabletStream(Stream stream, string filename, Action<IEnumerable<IKeyValuePair>> callback) {
            var reader = new TabletReader();

            var header = reader.ReadHeader(stream);
            if (header.Magic != Constants.TabletMagic) {
                // Not a tablet.
                throw new TabletValidationException("bad magic");
            }

            if (header.Version < 1) {
                throw new TabletValidationException("bad version: " + header.Version);
            }

            var filepath = Path.Combine(Dir, filename);
            var fs = new FileStream(filepath, FileMode.Create, FileAccess.Write);

            using (var writer = new BinaryWriter(fs)) {
                // Write the header.
                writer.Write(header.Raw, 0, header.Raw.Length);

                while (!EndOfBlocks(stream) && stream.Position < stream.Length) {
                    var blockData = reader.ReadBlock(stream);

                    // Write the block as-is.
                    writer.Write(blockData.Info.Raw, 0, blockData.Info.Raw.Length);
                    writer.Write(blockData.Raw, 0, blockData.Raw.Length);

                    if (blockData.Info.Checksum != 0 && blockData.Info.Checksum != blockData.Checksum) {
                        // Bad block checksum.
                        continue;
                    }

                    if (blockData.Info.Type != BlockType.Data) {
                        // Skip non-Data blocks.
                        continue;
                    }

                    var block = new Block((Slice)blockData.Data);
                    if (callback != null) {
                        callback(block.Find());
                    }
                }

                // Read/write the remainder in 4KiB chunks.
                var size = 4096;
                var buf = new byte[size];
                int read = 0;
                while ((read = stream.Read(buf, 0, size)) != 0) {
                    writer.Write(buf, 0, read);
                }
            }

            PushTablet(filepath);
        }

        public void PushTablet(string filename) {
            var t = new FileTablet(new FileStream(filename, FileMode.Open, FileAccess.Read));
            tablets.Add(t);
        }

        public void PopTablet() {
            if (tablets.Count == 0) {
                return;
            }
            var t = tablets[tablets.Count - 1];
            tablets.RemoveAt(tablets.Count - 1);
            t.Close();
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find(null);
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            var cmp = new EnumeratorCurrentKeyComparer();
            var set = new SortedSet<TabletEnumerator>(cmp);

            var index = 0;
            foreach (var t in tablets.Concat(mutableTablets)) {
                var e = t.Find(term).GetEnumerator();
                if (e.MoveNext()) {
                    var te = new TabletEnumerator();
                    te.TabletIndex = index;
                    te.Enumerator = e;
                    set.Add(te);
                }
                index += 1;
            }

            var prev = new byte[0];
            while (set.Count > 0) {
                // Remove the first enumerator.
                var te = set.Min;
                set.Remove(te);

                var key = te.Enumerator.Current.Key;
                if (prev.CompareKey(key) != 0 && !te.Enumerator.Current.IsDeleted) {
                    // Only yield keys we haven't seen.
                    yield return te.Enumerator.Current;
                }
                prev = key;

                // Re-add to the SortedList if we have more.
                if (te.Enumerator.MoveNext()) {
                    set.Add(te);
                }
            }

            yield break;
        }

        public Slice Get(Slice key) {
            foreach (var p in Find(key)) {
                if (Slice.Compare(p.Key, key) == 0) {
                    return p.Value;
                } else {
                    break;
                }
            }

            throw new KeyNotFoundException(key.ToUTF8String());
        }

        public void Put(Slice key, Slice val) {
            // Last mutableTablet is the writing tablet.
            var tablet = (MemoryTablet)mutableTablets[mutableTablets.Count - 1];
            tablet.Set(key, val);
        }

        public void Delete(Slice key) {
            // Last mutableTablet is the writing tablet.
            var tablet = (MemoryTablet)mutableTablets[mutableTablets.Count - 1];
            tablet.Delete(key);
        }

        private class EnumeratorCurrentKeyComparer : IComparer<TabletEnumerator> {
            public int Compare(TabletEnumerator x, TabletEnumerator y) {
                if (ReferenceEquals(x, y)) {
                    return 0;
                }

                var cmp = Slice.Compare(x.Enumerator.Current.Key, y.Enumerator.Current.Key);
                if (cmp == 0) {
                    // Key is the same, the newer (higher index) tablet wins.
                    return y.TabletIndex - x.TabletIndex;
                }
                return cmp;
            }
        }

        private class TabletEnumerator {
            public int TabletIndex;
            public IEnumerator<IKeyValuePair> Enumerator;
        }
    }
}
