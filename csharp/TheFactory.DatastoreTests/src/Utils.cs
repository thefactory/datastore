using System;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {

    public class Utils {
        public static Slice Slice(string str) {
            return (Slice)System.Text.Encoding.UTF8.GetBytes(str);
        }

        public static Pair Pair(Slice key, Slice value) {
            var p = new Pair();
            p.Key = key;
            p.Value = value;
            p.IsDeleted = false;
            return p;
        }

        private static String alnum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        public static String RandomString(int len) {
            var str = new char[len];
            var random = new Random();

            for (int i=0; i<len; i++) {
                str[i] = alnum[random.Next(alnum.Length)];
            }

            return new String(str);
        }
    }
}

