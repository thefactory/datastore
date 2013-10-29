using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.IO;
using System.Linq;

namespace TheFactory.Datastore {
    internal class FileManager {
        public string Dir { get; private set; }
 
        const string StackFilename = "stack.txt";
        private ObservableCollection<string> stackFiles;

        public FileManager(string dir) {
            Dir = dir;
            LoadStackFile();
        }

        private void LoadStackFile() {
            stackFiles = new ObservableCollection<string>();
            var path = Path.Combine(Dir, StackFilename);
            var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read);
            using (var reader = new StreamReader(fs)) {
                string line;
                while ((line = reader.ReadLine()) != null) {
                    stackFiles.Add(line.Trim());
                }
            }
            stackFiles.CollectionChanged += WriteStackFile;
        }

        private void WriteStackFile(object sender, NotifyCollectionChangedEventArgs args) {
            var path = Path.Combine(Dir, StackFilename);
            var fs = new FileStream(path, FileMode.Create, FileAccess.Write);
            using (var writer = new StreamWriter(fs)) {
                foreach (var filename in stackFiles) {
                    writer.WriteLine(filename.Trim());
                }
            }
        }

        public ReadOnlyCollection<string> TabletFileStack {
            get {
                return stackFiles.ToList().AsReadOnly();
            }
        }

        public void PushTabletFile(string val) {
            stackFiles.Add(val);
        }

        public string PopTabletFile() {
            if (stackFiles.Count < 1) {
                return null;
            }
            var ret = stackFiles[stackFiles.Count - 1];
            stackFiles.RemoveAt(stackFiles.Count - 1);
            return ret;
        }
    }
}
