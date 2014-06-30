using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TheFactory.Datastore {

    public interface IDatabase : IDisposable {
        Task<IEnumerable<IKeyValuePair>> Find(Slice term);

        Task<Slice> Get(Slice key);
        Task Put(Slice key, Slice val);
        Task Delete(Slice key);

        event EventHandler<KeyValueChangedEventArgs> KeyValueChangedEvent;

        Task Close();
    }

    internal interface ITablet {
        void Close();

        IEnumerable<IKeyValuePair> Find(Slice term);

        string Filename { get; }
    }

    public static class IDatabaseExtensions {

        public static async Task<IEnumerable<IKeyValuePair>> FindByPrefix(this IDatabase db, Slice term) {
            return (await db.Find(term)) 
                .Where(x => Slice.IsPrefix(x.Key, term));
        }

        public static async Task<IEnumerable<IKeyValuePair>> FindByPrefix(this IDatabase db, string term) {
            return await db.FindByPrefix((Slice)Encoding.UTF8.GetBytes(term));
        }

        public static async Task<Slice> Get(this IDatabase db, string key) {
            return await db.Get((Slice)Encoding.UTF8.GetBytes(key));
        }

        public static async Task<string> GetString(this IDatabase This, string key) {
            var slice = await This.Get((Slice)Encoding.UTF8.GetBytes(key));
            if (slice == null) {
                return null;
            }

            return slice.GetString();
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

