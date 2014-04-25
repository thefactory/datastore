using System;
using NUnit.Framework;
using System.Text;
using System.IO;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests {

    [TestFixture]
    public class StreamTabletTests {
        void VerifyKVs(string tabletFile, string expectedFile) {
            var enc = new UTF8Encoding();

            using (var stream = new FileStream(Helpers.TestFile(tabletFile), FileMode.Open, FileAccess.Read)) {
                var tablet = new StreamTablet(stream, new TabletReaderOptions());
                using (var data = new StreamReader(Helpers.TestFile(expectedFile))) {
                    foreach (var kv in tablet.Find(Slice.Empty)) {
                        var expected = data.ReadLine().Split(new char[] { ' ' });
                        var exKey = enc.GetBytes(expected[0]);
                        var exValue = enc.GetBytes(expected[1]);

                        Assert.True(kv.Key.Equals((Slice)exKey));
                        Assert.True(kv.Value.Equals((Slice)exValue));
                    }

                    Assert.True(data.ReadLine() == null);
                }
            }
        }

        [Test]
        public void TestStreams() {
            string[] testTablets = new string[] {
                "ngrams1/ngrams1-1block-uncompressed.tab",
                "ngrams1/ngrams1-1block-compressed.tab",
                "ngrams1/ngrams1-Nblock-compressed.tab",
            };

            foreach (var tablet in testTablets) {
                VerifyKVs(tablet, "ngrams1/ngrams1.txt");
            }
        }
    }
}

