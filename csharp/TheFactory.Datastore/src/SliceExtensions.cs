using System;
using System.Text;

namespace TheFactory.Datastore {

    public static class SliceExtensions {
        public static string GetString(this Slice This) {
            // Decode this Slice as a UTF-8 string.
            return Encoding.UTF8.GetString(This.Array, This.Offset, This.Length);
        }
    }
}

