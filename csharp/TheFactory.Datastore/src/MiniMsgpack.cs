using System;
using System.IO;

namespace TheFactory.Datastore {
    // these are copied from MsgPackCode, which is inaccessible
    public class MiniMsgpackCode {
        public const int NilValue = 0xc0;
        public const int MinimumFixedRaw = 0xa0;
        public const int Raw16 = 0xda;
        public const int Raw32 = 0xdb;

        public const int MaximumFixedPos = 0x7f;
        public const int UnsignedInt8 = 0xcc;
        public const int UnsignedInt16 = 0xcd;
        public const int UnsignedInt32 = 0xce;
        public const int UnsignedInt64 = 0xcf;
    }

    public class MiniMsgpack {
        public static int WriteRawLength(Stream stream, int length) {
            if (length < 32) {
                stream.WriteByte((byte)(MiniMsgpackCode.MinimumFixedRaw | length));
                return 1;
            } else if (length < 65536) {
                stream.WriteByte((byte)MiniMsgpackCode.Raw16);
                for (int i = 0; i < 2; i++) {
                    stream.WriteByte((byte)length);
                    length = length >> 8;
                }
                return 3;
            } else {
                stream.WriteByte((byte)MiniMsgpackCode.Raw32);
                for (int i = 0; i < 4; i++) {
                    stream.WriteByte((byte)length);
                    length = length >> 8;
                }
                return 5;
            }
        }

        public static int UnpackUInt32(Stream stream) {
            int num = 0;

            int flag = stream.ReadByte();
            if (flag <= MiniMsgpackCode.MaximumFixedPos) {
                return (int)flag;
            } else if (flag == MiniMsgpackCode.UnsignedInt16) {
                for (int i = 0; i < 2; i++) {
                    num = (num << 8) | (byte)stream.ReadByte();
                }
            } else if (flag == MiniMsgpackCode.UnsignedInt32) {
                for (int i = 0; i < 4; i++) {
                    num = (num << 8) | (byte)stream.ReadByte();
                }
            }

            return num;
        }

        public static int UnpackRawLength(int flag, Stream stream) {
            int length = 0;

            if ((flag & 0xe0) == MiniMsgpackCode.MinimumFixedRaw) {
                length = (int)(flag & 0x1f);
            } else if (flag == MiniMsgpackCode.Raw16) {
                for (int i = 0; i < 2; i++) {
                    length = (length << 8) | (byte)stream.ReadByte();
                }
            } else if (flag == MiniMsgpackCode.Raw32) {
                for (int i = 0; i < 4; i++) {
                    length = (length << 8) | (byte)stream.ReadByte();
                }
            } else {
                throw new MsgPack.InvalidMessagePackStreamException("Unexpected raw flag byte: " + flag);
            }

            return length;
        }
    }
}

