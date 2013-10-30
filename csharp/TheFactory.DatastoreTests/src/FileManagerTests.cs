using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {
    internal class FileManagerTestTablet : ITablet {
        public FileManagerTestTablet(string filename) {
            Filename = filename;
        }

        public void Close() {
        }

        public IEnumerable<IKeyValuePair> Find() {
            yield break;
        }

        public IEnumerable<IKeyValuePair> Find(Slice term) {
            yield break;
        }

        public string Filename { get; private set; }
    }

    [TestFixture]
    public class FileManagerTests {
        private FileManager fm;

        [SetUp]
        public void SetUp() {
            var path = Path.Combine(Path.GetTempPath(), "test");
            Directory.CreateDirectory(path);
            fm = new FileManager(path);
        }

        [TearDown]
        public void TearDown() {
            Directory.Delete(fm.Dir, true);
        }

        [Test]
        public void TestReadEmptyStack() {
            Assert.True(fm.ReadTabletStackFile().Count == 0);
        }

        [Test]
        public void TestReadWriteStack() {
            Assert.True(fm.ReadTabletStackFile().Count == 0);
            var s = new List<ITablet>() {
                new FileManagerTestTablet("foo"),
                new FileManagerTestTablet("bar"),
                new FileManagerTestTablet("baz")
            };
            fm.WriteTabletStackFile(s);
            var stack = fm.ReadTabletStackFile();
            Assert.True(stack.Count == s.Count);
            for (var i = 0; i < s.Count; i++) {
                Assert.True(stack[i] == s[i].Filename);
            }
        }
    }
}
