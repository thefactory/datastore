using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {
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
        public void TestGetEmptyStack() {
            Assert.True(fm.TabletFileStack.Count == 0);
        }

        [Test]
        public void TestStackOne() {
            var s = new List<string>() {"foo"};
            foreach (var val in s) {
                fm.PushTabletFile(val);
            }
            Assert.True(fm.TabletFileStack.Count == s.Count);
            Assert.True(fm.TabletFileStack[0] == s[0]);
        }

        [Test]
        public void TestStackMany() {
            var s = new List<string>() {"foo", "bar", "baz"};
            foreach (var val in s) {
                fm.PushTabletFile(val);
            }
            Assert.True(fm.TabletFileStack.Count == s.Count);
            for (var i = 0; i < s.Count; i++) {
                Assert.True(fm.TabletFileStack[i] == s[i]);
            }
        }

        [Test]
        public void TestStackPopEmpty() {
            Assert.True(fm.PopTabletFile() == null);
        }

        [Test]
        public void TestStackPushPop() {
            var s = new List<string>() {"foo", "bar", "baz"};
            foreach (var val in s) {
                fm.PushTabletFile(val);
            }
            var count = 0;
            string str;
            while ((str = fm.PopTabletFile()) != null) {
                count += 1;
                Assert.True(str == s[s.Count - count]);
            }
            Assert.True(s.Count == count);
            Assert.True(fm.TabletFileStack.Count == 0);
        }
    }
}
