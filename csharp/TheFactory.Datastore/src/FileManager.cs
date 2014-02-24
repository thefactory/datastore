using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace TheFactory.Datastore {
    // abstract out filesystem operations
    public interface IFileSystem {
        // create a file for writing, truncating if it exists
        Stream Create(string name);

        // open a file for reading
        Stream Open(string name);

        // open a file for appending: this can be removed once we support multiple write logs
        Stream Append(string name);

        bool Exists(string name);

        void Remove(string name);
        void Rename(string oldname, string newname);
        void Mkdirs(string path);

        // returns a Disposable representing a locked file; raises IOException if unable to lock
        IDisposable Lock(string name);

        // return a list of filenames in dir. returned names are relative to dir
        string[] List(string dir);
    }

    public class MemFileSystem: IFileSystem {
        private Dictionary<String, Mutex> locks = new Dictionary<String, Mutex>();

        public Stream Create(string name) {
            return Stream.Null;
        }

        public Stream Open(string name) {
            return Stream.Null;
        }

        public Stream Append(string name) {
            return Create(name);
        }

        public bool Exists(string name) {
            return false;
        }

        public void Remove(string name) {
        }

        public void Rename(string oldname, string newname) {
        }

        public void Mkdirs(string path) {
        }

        public IDisposable Lock(string name) {
            lock (locks) {
                Mutex mutex;
                if (!locks.ContainsKey(name)) {
                    mutex = new Mutex(true);
                    locks.Add(name, mutex);
                } else {
                    mutex = locks[name];
                    if (mutex.WaitOne(0)) {
                        throw new IOException(String.Format("Lock already held: {0}", name));
                    }
                }

                return mutex;
            }
        }

        public string[] List(string dir) {
            return new string[0];
        }
    }

    public class FileSystem: IFileSystem {
        public Stream Create(string name) {
            return new FileStream(name, FileMode.Create, FileAccess.Write, FileShare.None);
        }

        public Stream Open(string name) {
            return new FileStream(name, FileMode.Open, FileAccess.Read, FileShare.Read);
        }

        public Stream Append(string name) {
            return new FileStream(name, FileMode.Append, FileAccess.Write, FileShare.None);
        }

        public bool Exists(string name) {
            return File.Exists(name);
        }

        public void Remove(string name) {
            File.Delete(name);
        }

        public void Rename(string oldname, string newname) {
            File.Move(oldname, newname);
        }

        public void Mkdirs(string path) {
            Directory.CreateDirectory(path);
        }

        public IDisposable Lock(string name) {
            return new FileStream(name, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None);
        }

        public string[] List(string dir) {
            var files = Directory.GetFiles(dir, "*", SearchOption.TopDirectoryOnly);
            for (int i=0; i<files.Length; i++) {
                files[i] = Path.GetFileName(files[i]);
            }

            return files;
        }
    }

    internal class FileManager {
        public string Dir { get; private set; }
 
        const string LockFile = "access.lock";
        const string TabletWriteLog = "write.log";
        const string ImmutableWriteLog = "write_imm.log";
        const string TabletStackFilename = "stack.txt";

        public FileManager(string dir) {
            Dir = dir;
        }

        public string DbFilename(string filename) {
            return Path.Combine(Dir, filename);
        }

        public string GetLockFile() {
            return DbFilename(LockFile);
        }

        public string GetTransactionLog() {
            return DbFilename(TabletWriteLog);
        }

        public string GetImmutableTransactionLog() {
            return DbFilename(ImmutableWriteLog);
        }

        public string GetTabletStack() {
            return DbFilename(TabletStackFilename);
        }

        public List<string> ReadTabletStackFile() {
            var stackFiles = new List<string>();
            var path = Path.Combine(Dir, TabletStackFilename);
            var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read);
            using (var reader = new StreamReader(fs)) {
                string line;
                while ((line = reader.ReadLine()) != null) {
                    stackFiles.Add(line.Trim());
                }
            }
            return stackFiles;
        }

        public void WriteTabletStackFile(IEnumerable<ITablet> tablets) {
            var path = Path.Combine(Dir, TabletStackFilename);
            var fs = new FileStream(path, FileMode.Create, FileAccess.Write);
            using (var writer = new StreamWriter(fs)) {
                foreach (var t in tablets) {
                    if (t.Filename != null) {
                        writer.WriteLine(t.Filename.Trim());
                    }
                }
            }
        }
    }
}
