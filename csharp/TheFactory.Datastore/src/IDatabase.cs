using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace TheFactory.Datastore {

    public interface IDatabase : IDisposable {
        IEnumerable<IKeyValuePair> Find(Slice term);

        void PushTablet(string filename);
        void PushTabletStream(Stream stream, Action<IEnumerable<IKeyValuePair>> callback);

        Slice Get(Slice key);
        void Put(Slice key, Slice val);
        void Delete(Slice key);

        void Close();
    }

    public static class IDatabaseExtensions {
        public static IEnumerable<IKeyValuePair> Find(this IDatabase db) {
            return db.Find(null);
        }

        public static IEnumerable<IKeyValuePair> FindByPrefix(this IDatabase db, Slice term) {
            foreach (var kv in db.Find(term)) {
                if (!Slice.IsPrefix(kv.Key, term)) {
                    break;
                }
                yield return kv;
            }
            yield break;
        }

        public static IEnumerable<IKeyValuePair> FindByPrefix(this IDatabase db, string term) {
            return db.FindByPrefix((Slice)Encoding.UTF8.GetBytes(term));
        }

        public static Slice Get(this IDatabase db, string key) {
            return db.Get((Slice)Encoding.UTF8.GetBytes(key));
        }

        public static void Put(this IDatabase db, string key, Slice val) {
            db.Put((Slice)Encoding.UTF8.GetBytes(key), val);
        }

        public static void Put(this IDatabase db, string key, string val) {
            db.Put((Slice)Encoding.UTF8.GetBytes(key), (Slice)Encoding.UTF8.GetBytes(val));
        }

        public static void Delete(this IDatabase db, string key) {
            db.Delete((Slice)Encoding.UTF8.GetBytes(key));
        }
    }
}

