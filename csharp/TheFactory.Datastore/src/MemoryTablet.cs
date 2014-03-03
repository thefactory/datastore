using System;
using System.Threading;
using System.Collections.Generic;

namespace TheFactory.Datastore {

    internal class MemoryTablet: ITablet {
        const int maxHeight = 12;
        const int fKey = 0;
        const int fVal = 1;
        const int fNxt = 2;

        const int zeroNode = 0;
        const int headNode = -fNxt;

        const int kvOffsetEmptySlice = -1;
        const int kvOffsetDeletedNode = -2;

        object rwLock = new object();
        Random rand = new Random();

        public int ApproxSize { get { return kvData.Count + nodeData.Count; } }
        public string Filename { get { return null; } }

        int height;
        List<byte> kvData;
        List<int> nodeData;

        public static Slice Tombstone = (Slice)(new byte[] {0x74, 0x6f, 0x6d, 0x62});

        public MemoryTablet() {
            height = 1;
            kvData = new List<byte>(4096);
            nodeData = new List<int>(256);

            // Create the root node.
            for (int i = 0; i < maxHeight; i++) {
                nodeData.Add(0);
            }
        }

        public void Apply(Batch batch) {
            if (batch.IsEmpty()) {
                return;
            }

            lock(rwLock) {
                foreach (IKeyValuePair kv in batch.Pairs()) {
                    if (kv.IsDeleted) {
                        Delete(kv.Key.Detach());
                    } else {
                        Set(kv.Key.Detach(), kv.Value.Detach());
                    }
                }
            }
        }

        Slice Load(int kvOffset) {
            if (kvOffset < 0) {
                return null;
            }

            int len = kvData[kvOffset + 0] << 24 | kvData[kvOffset + 1] << 16 | kvData[kvOffset + 2] << 8 | kvData[kvOffset + 3];
            return (Slice)kvData.GetRange(kvOffset + 4, len).ToArray();
        }

        int Save(Slice data) {
            int kvOffset = kvData.Count;
            int len = data.Length;

            kvData.Add((byte)(len >> 24));
            kvData.Add((byte)(len >> 16));
            kvData.Add((byte)(len >> 8));
            kvData.Add((byte)(len));

            kvData.AddRange(data.ToArray());

            return kvOffset;
        }

        internal void Set(Slice key, Slice value) {
            int[] prev = new int[maxHeight];

            var found = FindNode(key, prev);
            if (found.Item2) {
                // Match was exact.
                nodeData[found.Item1 + fVal] = Save(value);
            }

            // Create a new node: select its height, branching with 25% probability.
            int h = 1;
            while (h < maxHeight && rand.Next(4) == 0) {
                h++;
            }

            // Raise the skiplist's height if necessary.
            if (height < h) {
                for (int i = height; i < h; i++) {
                    prev[i] = headNode;
                }
                height = h;
            }

            // Insert the new node
            int offset = nodeData.Count;
            nodeData.Add(Save(key));

            if (ReferenceEquals(value, Tombstone)) {
                nodeData.Add(kvOffsetDeletedNode);
            } else {
                nodeData.Add(Save(value));
            }

            for (int i = 0; i < h; i++) {
                int j = prev[i] + fNxt + i;
                nodeData.Add(nodeData[j]);
                nodeData[j] = offset;
            }
        }

        internal void Delete(Slice key) {
            Set(key, Tombstone);
        }

        public void Close() {
            nodeData = null;
            kvData = null;
        }

        Tuple<int, bool> FindNode(Slice key, int[] prev) {
            int n = 0;
            bool exact = false;

            var p = headNode;
            for (var h = height-1; h >= 0; h--) {
                n = nodeData[p+fNxt+h];
                while (true) {
                    if (n == zeroNode) {
                        exact = false;
                        break;
                    }

                    var offset = nodeData[n+fKey];

                    int compare = Load(offset).CompareTo(key);
                    if (compare >= 0) {
                        exact = compare == 0;
                        break;
                    }

                    p = n;
                    n = nodeData[n+fNxt+h];
                }

                if (prev != null) {
                    prev[h] = p;
                }
            }

            return new Tuple<int, bool>(n, exact);
        }

        Slice Get(Slice key) {
            lock(rwLock) {
                var found = FindNode(key, null);
                if (!found.Item2) {
                    // Didn't find the exact key;
                    return null;
                }

                return Load(found.Item1);
            }
        }

        public IEnumerable<IKeyValuePair> Find() {
            return Find((Slice)null);
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            lock(rwLock) {
                var found = FindNode(term, null);
                var n = found.Item1;
                var pair = new Pair();

                while (n != zeroNode) {
                    pair.Reset();
                    pair.Key = Load(nodeData[n+fKey]);

                    if (nodeData[n+fVal] == kvOffsetDeletedNode) {
                        pair.IsDeleted = true;
                    } else {
                        pair.Value = Load(nodeData[n+fVal]);
                    }

                    yield return pair;
                    n = nodeData[n+fNxt];
                }
            }
        } 
    }

    internal class MemoryTablet2 : ITablet {
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

        public MemoryTablet2() {
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

        public IEnumerable<IKeyValuePair> Find() {
            return Find((Slice)null);
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

