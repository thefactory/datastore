using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {
    public class Database {
        private List<ITablet> tablets;
        private List<ITablet> mutableTablets;

        public Database() {
            tablets = new List<ITablet>();
            mutableTablets = new List<ITablet>();
            mutableTablets.Add(new MemoryTablet());
            // There's probably something that happens elsewhere which involves
            // replaying a durable log for an in-flight MemoryTablet.
        }

        public void Close() {
            while (tablets.Count > 0) {
                PopTablet();
            }
            // TODO: Not sure what to do with mutableTablets.
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

        public IEnumerable<IKeyValuePair> Find(byte[] term) {
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

        public byte[] Get(byte[] key) {
            foreach (var p in Find(key)) {
                if (p.Key.CompareKey(key) == 0) {
                    return p.Value;
                } else {
                    break;
                }
            }

            throw new KeyNotFoundException(key.StringifyKey());
        }

        public void Put(byte[] key, byte[] val) {
            // Last mutableTablet is the writing tablet.
            var tablet = (MemoryTablet)mutableTablets[mutableTablets.Count - 1];
            tablet.Set(key, val);
        }

        public void Delete(byte[] key) {
            // Last mutableTablet is the writing tablet.
            var tablet = (MemoryTablet)mutableTablets[mutableTablets.Count - 1];
            tablet.Delete(key);
        }

        private class EnumeratorCurrentKeyComparer : IComparer<TabletEnumerator> {
            public int Compare(TabletEnumerator x, TabletEnumerator y) {
                if (ReferenceEquals(x, y)) {
                    return 0;
                }

                var cmp = x.Enumerator.Current.Key.CompareKey(y.Enumerator.Current.Key);
                if (cmp == 0) {
                    // Key is the same, the newer (higher index) tablet wins.
                    if (x.TabletIndex < y.TabletIndex) {
                        return 1;
                    } else if (x.TabletIndex > y.TabletIndex) {
                        return -1;
                    } else {
                        return 0;
                    }
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
