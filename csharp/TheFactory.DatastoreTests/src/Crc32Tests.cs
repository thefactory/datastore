using System;
using System.IO;
using NUnit.Framework;
using TheFactory.Datastore;

namespace TheFactory.DatastoreTests
{
	public class Test
	{
		public UInt32 expected;
		public String str;

		public Test (UInt32 expected, String str)
		{
			this.expected = expected;
			this.str = str;
		}
	}

	[TestFixture]
	public class Crc32Tests
	{
		[Test]
		public void TestKnownStringChecksums ()
		{
			Test[] tests = new Test[] {
				new Test (0x0, ""),
				new Test (0xe8b7be43, "a"),
				new Test (0x9e83486d, "ab"),
				new Test (0x352441c2, "abc"),
				new Test (0xed82cd11, "abcd"),
				new Test (0x8587d865, "abcde"),
				new Test (0x4b8e39ef, "abcdef"),
				new Test (0x312a6aa6, "abcdefg"),
				new Test (0xaeef2a50, "abcdefgh"),
				new Test (0x8da988af, "abcdefghi"),
				new Test (0x3981703a, "abcdefghij"),
				new Test (0x6b9cdfe7, "Discard medicine more than two years old."),
				new Test (0xc90ef73f, "He who has a shady past knows that nice guys finish last."),
				new Test (0xb902341f, "I wouldn't marry him with a ten foot pole."),
				new Test (0x042080e8, "Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"),
				new Test (0x154c6d11, "The days of the digital watch are numbered.  -Tom Stoppard"),
				new Test (0x4c418325, "Nepal premier won't resign."),
				new Test (0x33955150, "For every action there is an equal and opposite government program."),
				new Test (0x26216a4b, "His money is twice tainted: 'taint yours and 'taint mine."),
				new Test (0x1abbe45e, "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"),
				new Test (0xc89a94f7, "It's a tiny change to the code and not completely disgusting. - Bob Manchek"),
				new Test (0xab3abe14, "size:  a.out:  bad magic"),
				new Test (0xbab102b6, "The major problem is with sendmail.  -Mark Horton"),
				new Test (0x999149d7, "Give me a rock, paper and scissors and I will move the world.  CCFestoon"),
				new Test (0x6d52a33c, "If the enemy is within range, then so are you."),
				new Test (0x90631e8d, "It's well we cannot hear the screams/That we create in others' dreams."),
				new Test (0x78309130, "You remind me of a TV show, but that's all right: I watch it anyway."),
				new Test (0x7d0a377f, "C is as portable as Stonehedge!!"),
				new Test (0x8c79fd79, "Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"),
				new Test (0xa20b7167, "The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"),
				new Test (0x8e0bb443, "How can you write a big system without C++?  -Paul Glick")
			};

			for (int i=0; i<tests.Length; i++) {
				Test test = tests [i];
				byte[] data = System.Text.Encoding.ASCII.GetBytes (test.str);

				Assert.AreEqual (test.expected, Crc32.ChecksumIeee (data));
			}
		}
	}
}

