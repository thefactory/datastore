using System;
using System.Threading;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TheFactory.Datastore {

    public class MemoryTablet : ITablet {
        private OurSortedDictionary<Slice, Slice> backing;
        private AsyncLock backingLock = new AsyncLock();

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
            ApproxSize = 0;
        }

        public async Task Apply(Batch batch) {
            if (batch.IsEmpty()) {
                return;
            }

            foreach (IKeyValuePair kv in batch.Pairs()) {
                if (kv.IsDeleted) {
                    await Delete(kv.Key.Detach());
                    ApproxSize += kv.Key.Length;
                } else {
                    await Set(kv.Key.Detach(), kv.Value.Detach());
                    ApproxSize += kv.Key.Length + kv.Value.Length;
                }
            }
        }

        public async Task Set(Slice key, Slice val) {
            using (await backingLock.LockAsync()) {
                backing[key] = val;
            }
        }

        public async Task Delete(Slice key) {
            await Set(key, Tombstone);
        }

        public void Close() {
            backing.Clear();
        }

        public IAsyncEnumerable<IKeyValuePair> Find(Slice term) {
            if (backing.Count == 0) {
                return AsyncEnumerable.Empty<IKeyValuePair>();
            }

            return AsyncEnumerableEx.Return(() => backingLock.LockAsync())
                .SelectMany(l => {
                    IEnumerator<KeyValuePair<Slice, Slice>> items;

                    if (term == null) {
                        items = backing.GetEnumerator();
                    } else {
                        // skip all elements less than term
                        items = backing.GetSuffixEnumerator(term);
                    }

                    return AsyncEnumerable.Generate(new Pair(), _ => {
                        if (items.MoveNext()) return true;

                        l.Dispose();
                        return false;
                    }, ret => {
                        var kv = items.Current;

                        ret.Reset();
                        ret.Key = kv.Key;
                        if (ReferenceEquals(kv.Value, Tombstone)) {
                            ret.IsDeleted = true;
                        } else {
                            ret.Value = kv.Value;
                        }

                        return ret;
                    }, x => x);
                });
        }

        private class KeyComparer : IComparer<Slice> {
            public int Compare(Slice x, Slice y) {
                return Slice.Compare(x, y);
            }
        }
    }

}

