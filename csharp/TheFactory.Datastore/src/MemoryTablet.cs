using System;
using System.Threading;
using System.Collections.Generic;

namespace TheFactory.Datastore {

    internal class MemoryTablet : ITablet {
        private OurSortedDictionary<Slice, Slice> backing;
        private object backingLock;

        public int ApproxSize { get; private set; }

        public string Filename {
            get {
                return null;
            }
        }

        // Deleted key marker -- Get() and the Find() enumerator
        // should check against this reference and set KV.IsDeleted
        // when appropriate.
        public static Slice Tombstone = (Slice)(new byte[] {0x74, 0x6f, 0x6d, 0x62});

        public MemoryTablet() {
            backing = new OurSortedDictionary<Slice, Slice>(new KeyComparer());
            backingLock = new ReaderWriterLockSlim();
            ApproxSize = 0;
        }

        public void Apply(Batch batch) {
            if (batch.IsEmpty()) {
                return;
            }

            foreach (IKeyValuePair kv in batch.Pairs()) {
                if (kv.IsDeleted) {
                    Delete(kv.Key.Detach());
                    ApproxSize += kv.Key.Length;
                } else {
                    Set(kv.Key.Detach(), kv.Value.Detach());
                    ApproxSize += kv.Key.Length + kv.Value.Length;
                }
            }
        }

        public void Set(Slice key, Slice val) {
            lock (backingLock) {
                backing[key] = val;
            }
        }

        public void Delete(Slice key) {
            Set(key, Tombstone);
        }

        public void Close() {
            backing.Clear();
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            if (backing.Count == 0) {
                yield break;
            }

            lock (backingLock) {
                IEnumerator<KeyValuePair<Slice, Slice>> items;

                if (term == null) {
                    items = backing.GetEnumerator();
                } else {
                    // skip all elements less than term
                    items = backing.GetSuffixEnumerator(term);
                }

                var ret = new Pair();
                while (items.MoveNext()) {
                    var kv = items.Current;

                    ret.Reset();
                    ret.Key = kv.Key;
                    if (ReferenceEquals(kv.Value, Tombstone)) {
                        ret.IsDeleted = true;
                    } else {
                        ret.Value = kv.Value;
                    }

                    yield return ret;
                }
            }

            yield break;
        }

        private class KeyComparer : IComparer<Slice> {
            public int Compare(Slice x, Slice y) {
                return Slice.Compare(x, y);
            }
        }
    }

}

