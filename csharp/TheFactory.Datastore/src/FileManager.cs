using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Splat;

namespace TheFactory.Datastore {
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
            using (var fs = Locator.Current.GetService<IFileSystem>().GetStream(path, FileMode.OpenOrCreate, FileAccess.Read))
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
            using (var fs = Locator.Current.GetService<IFileSystem>().GetStream(path, FileMode.Create, FileAccess.Write))
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
