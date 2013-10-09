using System;
using System.Collections.Generic;
using System.IO;
using TheFactory.Datastore.Helpers;

namespace TheFactory.Datastore {
    public class Database {
        private List<ITablet> tablets;

        public Database() {
            tablets = new List<ITablet>();
        }

        public void Close() {
            while (tablets.Count > 0) {
                PopTablet();
            }
        }

        public void PushTablet(string filename) {
            var t = new FileTablet(new FileStream(filename, FileMode.Open));
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
            if (tablets.Count == 0) {
                yield break;
            }

            if (tablets.Count == 1) {
                foreach (var p in tablets[0].Find(term)) {
                    yield return p;
                }
                yield break;
            }

            var cmp = new EnumeratorCurrentKeyComparer();
            var set = new SortedSet<TabletEnumerator>(cmp);

            var index = 0;
            foreach (var t in tablets) {
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
                if (prev.CompareKey(key) != 0) {
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

        private class EnumeratorCurrentKeyComparer : IComparer<TabletEnumerator> {
            public int Compare(TabletEnumerator x, TabletEnumerator y) {
                if (ReferenceEquals(x, y)) {
                    return 0;
                }

                var cmp = x.Enumerator.Current.Key.CompareKey(y.Enumerator.Current.Key);
                if (cmp == 0) {
                    // Key is the same, the newer (lower index) tablet wins.
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
