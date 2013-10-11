using System;

namespace TheFactory.Datastore
{
	static class Polynomials
	{
		public const UInt32 Ieee = 0xedb88320;
	}

	public class Crc32
	{
		private static UInt32[] IeeeTable = makeTable(Polynomials.Ieee);

		private UInt32[] table;

		public Crc32(UInt32[] table)
		{
			this.table = table;
		}

		public UInt32 Update(UInt32 crc, byte[] data)
		{
			crc = ~crc;
			for (int i=0; i<data.Length; i++) {
				crc = table[((byte)crc)^data[i]] ^ (crc >> 8);
			}
			return ~crc;
		}

		public static UInt32 ChecksumIeee(byte[] data)
		{
			return new Crc32(IeeeTable).Update(0, data);
		}

		static UInt32[] makeTable(UInt32 poly) {
			UInt32[] table = new UInt32[256];

			for (int i=0; i<256; i++) {
				UInt32 crc = (UInt32)i;
				for (int j=0; j<8; j++) {
					if ((crc & 1) == 1) {
						crc = (crc >> 1) ^ poly;
					} else {
						crc >>= 1;
					}
				}
				table[i] = crc;
			}

			return table;
		}
	}
}

