using System;
using System.Text;

namespace TheFactory.Datastore {

    public class Slice : IComparable, IComparable<Slice> {
        private byte[] array;
        private int offset;
        private int length;

        public Slice (byte[] array, int offset, int length) {
            this.array = array;
            this.offset = offset;
            this.length = length;
        }

        public byte[] Array { get { return array; } }
        public int Offset { get { return offset; } }
        public int Length { get { return length; } }

        public static implicit operator byte[](Slice s) {
            return s.ToArray();
        }

        public static explicit operator Slice(byte[] array) {
            return new Slice(array, 0, array.Length);
        }

        public Slice Subslice(int skip) {
            if (skip < 0) {
                // slicing from [-5:] will always produce a 5-element slice
                return Subslice(skip, -skip);
            }

            return Subslice(skip, this.Length - skip);
        }

        public Slice Subslice(int skip, int length) {
            int newOffset;
            if (skip < 0) {
                newOffset = this.offset + this.length + skip;
            } else {
                newOffset = this.offset + skip;
            }

            return new Slice(this.array, newOffset, length);
        }

        public Slice Detach() {
            return (Slice)this.ToArray();
        }

        public byte[] ToArray() {
            if (this.Offset == 0 && this.Length == Array.Length) {
                return Array;
            }

            byte[] array = new byte[this.Length];
            Buffer.BlockCopy(this.Array, this.Offset, array, 0, this.Length);
            return array;
        }

        public bool Equals(Slice that) {
            return Compare(this, that) == 0;
        }

        public static int Compare(Slice x, Slice y) {
            if (ReferenceEquals(x, y)) {
                return 0;
            }

            if (x == null) {
                return -1;
            }

            if (y == null) {
                return 1;
            }

            var end = Math.Min(x.Length, y.Length);

            for (int xi = x.Offset, yi = y.Offset; xi < end; xi++, yi++) {
                if (x.Array[xi] != y.Array[yi]) {
                    return x.Array[xi] - y.Array[yi];
                }
            }

            return x.Length - y.Length;
        }

        public int CommonBytes(Slice that) {
            if (that == null) {
                return 0;
            }

            var end = Math.Min(this.Length, that.Length);

            int n;
            for (n = 0; n < end; n++) {
                if (this.Array[n] != that.Array[n]) {
                    break;
                }
            }
            return n;
        }

        public string ToUTF8String() {
            // safely stringify the slice data
            var raw = BitConverter.ToString((byte[])this);
            try {
                var str = Encoding.UTF8.GetString((byte[])this);
                return String.Format("{0} ({1})", raw, str);
            } catch (ArgumentException) {
                return raw;
            }
        }

        // implement IComparable
        public int CompareTo(Object obj) {
            Slice that = obj as Slice;
            if (that == null) {
                throw new ArgumentException("Object is not a Slice");
            }

            return Compare(this, that);
        }

        // implements IComparable<Slice>
        public int CompareTo(Slice that) {
            return Compare(this, that);
        }
    }
}

