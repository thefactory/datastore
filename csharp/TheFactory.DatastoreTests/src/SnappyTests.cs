using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using TheFactory.Datastore;
using Snappy.Sharp;

namespace TheFactory.DatastoreTests
{
	[TestFixture]
	public class SnappyTests
	{
		[Test]
		public void TestBrokenLength()
		{
			// round-trip a previously bad block through a Snappy Compress/Decompress cycle.
			// This is entirely a Snappy.Sharp test but it's more convenient for us to own it.
			var c = new Snappy.Sharp.SnappyCompressor();
			var b = new BlockWriter(10);

			String[][] data = {
				new String[]{ "yesterday,	the	floor", "1" },
				new String[]{ "yesterday,	thinking	which", "1" },
				new String[]{ "yesterday’s	pain,	projects", "1" },
				new String[]{ "yesteryears	and	care", "1" },
				new String[]{ "yet	\"proved\")	by", "1" },
				new String[]{ "yet	\"the	'barbarians'", "1" },
				new String[]{ "yet	(but	which", "1" },
				new String[]{ "yet	(the	question", "1" },
				new String[]{ "yet	-	the", "1" },
				new String[]{ "yet	22	and", "1" },
				new String[]{ "yet	66)	to", "1" },
				new String[]{ "yet	ABC	could", "1" },
				new String[]{ "yet	Abby	feels", "1" },
				new String[]{ "yet	Armstrong	reportedly", "1" },
				new String[]{ "yet	Blizzard	was", "1" },
				new String[]{ "yet	Britain,	but", "1" },
				new String[]{ "yet	Buddhism	had", "1" },
				new String[]{ "yet	Danzig	and", "1" },
				new String[]{ "yet	Dogpatch	gals", "1" },
				new String[]{ "yet	Ford	Motor", "1" },
				new String[]{ "yet	I	cannot", "1" },
				new String[]{ "yet	I	saw", "1" },
				new String[]{ "yet	I	say", "1" },
				new String[]{ "yet	Jean,	as", "1" },
				new String[]{ "yet	Maclean	did", "1" },
				new String[]{ "yet	Morton's	accomplishments", "1" },
				new String[]{ "yet	Pulaski	is", "1" },
				new String[]{ "yet	Sepp	Maier", "1" },
				new String[]{ "yet	Ship	breaking,", "1" },
				new String[]{ "yet	Truman	was", "1" },
				new String[]{ "yet	Turkish	speaking", "1" },
				new String[]{ "yet	Zardoz's	aim", "1" },
				new String[]{ "yet	Zhang's	proposal", "1" },
				new String[]{ "yet	a	Christian,", "1" },
				new String[]{ "yet	a	city)", "1" },
				new String[]{ "yet	a	commercial", "1" },
				new String[]{ "yet	a	consensus", "2" },
				new String[]{ "yet	a	detailed", "1" },
				new String[]{ "yet	a	flood", "1" },
				new String[]{ "yet	a	further", "1" },									
			};

			for (int i = 0; i < data.Length; i++) {
				var key = System.Text.Encoding.ASCII.GetBytes (data [i] [0]);
				var value = System.Text.Encoding.ASCII.GetBytes (data [i] [1]);
                b.Append ((Slice)key, (Slice)value);
			}

			var block = b.Finish();

			var compressBuf = new byte[c.MaxCompressedLength (block.Buffer.Length)];
			var compressLen = c.Compress(block.Buffer, 0, block.Buffer.Length, compressBuf);

			compressBuf = compressBuf.Take(compressLen).ToArray ();

			var d = new Snappy.Sharp.SnappyDecompressor();
			var decompressed = d.Decompress(compressBuf, 0, compressLen);

            Assert.True(((byte[])block.Buffer).CompareBytes(0, decompressed, 0, decompressed.Length));
		}
	}
}

