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

        public int ApproxSize { get { return kvDataCount + nodeDataCount; } }
        public string Filename { get { return null; } }

        int height = 1;

        byte[] kvData;
        int kvDataCount;
        int[] nodeData;
        int nodeDataCount;

        public static Slice Tombstone = (Slice)(new byte[] {0x74, 0x6f, 0x6d, 0x62});

        public MemoryTablet() {
            height = 1;
            kvData = new byte[4096];
            nodeData = new int[256];

            kvDataCount = 0;
            nodeDataCount = maxHeight;
        }

        public void Apply(Batch batch) {
            if (batch.IsEmpty()) {
                return;
            }

            lock(rwLock) {
                foreach (IKeyValuePair kv in batch.Pairs()) {
                    if (kv.IsDeleted) {
                        Delete(kv.Key);
                    } else {
                        Set(kv.Key, kv.Value);
                    }
                }
            }
        }

        Slice Load(int kvOffset) {
            if (kvOffset < 0) {
                return null;
            }

            int len = kvData[kvOffset + 0] << 24 | kvData[kvOffset + 1] << 16 | kvData[kvOffset + 2] << 8 | kvData[kvOffset + 3];
            return new Slice(kvData, kvOffset + 4, len);
        }

        int Save(Slice data) {
            int kvOffset = kvDataCount;
            int len = data.Length;

            ensureCapacity(ref kvData, kvDataCount, 4 + data.Length);
            kvData[kvDataCount++] = (byte)(len >> 24);
            kvData[kvDataCount++] = (byte)(len >> 16);
            kvData[kvDataCount++] = (byte)(len >> 8);
            kvData[kvDataCount++] = (byte)(len);

            Array.Copy(data.Array, data.Offset, kvData, kvDataCount, data.Length);
            kvDataCount += data.Length;
            return kvOffset;
        }

        void ensureCapacity<T>(ref T[] array, int pos, int newLength) {
            if (newLength < array.Length - pos) {
                return;
            }

            int newCapacity = 2 * (pos + newLength);
            Array.Resize(ref array, newCapacity);
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

            ensureCapacity(ref nodeData, nodeDataCount, fNxt + h);

            // Insert the new node
            int offset = nodeDataCount;
            nodeData[nodeDataCount++] = Save(key);

            if (ReferenceEquals(value, Tombstone)) {
                nodeData[nodeDataCount++] = kvOffsetDeletedNode;
            } else {
                nodeData[nodeDataCount++] = Save(value);
            }

            for (int i = 0; i < h; i++) {
                int j = prev[i] + fNxt + i;
                nodeData[nodeDataCount++] = nodeData[j];
                nodeData[j] = offset;
            }
        }

        internal void Delete(Slice key) {
            Set(key, Tombstone);
        }

        public void Close() {
            kvData = null;
            nodeData = null;
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

