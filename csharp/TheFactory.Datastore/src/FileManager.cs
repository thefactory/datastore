using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace TheFactory.Datastore {
    internal class FileManager {
        public string Dir { get; private set; }
 
        const string TabletWriteLog = "write.log";
        const string TabletStackFilename = "stack.txt";

        public FileManager(string dir) {
            Dir = dir;
        }

        public string GetTransactionLog() {
            return Path.Combine(Dir, TabletWriteLog);
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
