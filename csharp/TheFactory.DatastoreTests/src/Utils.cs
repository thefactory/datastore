using System;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {

    public class Utils {
        public static Slice Slice(string str) {
            return (Slice)System.Text.Encoding.UTF8.GetBytes(str);
        }
    }
}

