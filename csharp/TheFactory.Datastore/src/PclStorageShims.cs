using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PCLStorage;

namespace TheFactory.Datastore {
    static class PclStorageShims {
        public static async Task<IFolder> CreateDirectoryRecursive(this IFileSystem fs, string path) {
            var currentFolder = default(IFolder);
            var currentPath = path;

            var separators = new[] { '\\', '/' };
            while ((currentFolder = await fs.GetFolderFromPathAsync(currentPath)) == null) {
                var idx = currentPath.LastIndexOfAny(separators);
                if (idx == -1) {
                    throw new ArgumentException("Can't find base folder or path is malformed", "path");
                }

                currentPath = currentPath.Substring(0, idx);
            }

            var toCreate = path.Replace(currentPath, "").Split('\\', '/');
            foreach (var v in toCreate) {
                currentFolder = await currentFolder.CreateFolderAsync(v, CreationCollisionOption.OpenIfExists);
            }

            return currentFolder;
        }

        public static async Task<IDisposable> FileLock(this IFileSystem fs, string path)
        {
            var dir = Path.GetDirectoryName(path);
            var folder = await fs.CreateDirectoryRecursive(dir);

            // TODO: PCLStorage doesn't let you specify exclusivity
            var fi = await folder.CreateFileAsync(Path.GetFileName(path), CreationCollisionOption.FailIfExists);
            return await fi.OpenAsync(FileAccess.ReadAndWrite);
        }
    }
}
