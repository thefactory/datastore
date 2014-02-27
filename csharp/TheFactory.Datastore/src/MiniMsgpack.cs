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

    public class MiniMsgpackException: Exception {
        public MiniMsgpackException(string str): base(str) {
        }
    }

    public class MiniMsgpack {
        public static int WriteRawLength(Stream stream, int length) {
            if (length < 32) {
                stream.WriteByte((byte)(MiniMsgpackCode.MinimumFixedRaw | length));
                return 1;
            } else if (length < 65536) {
                stream.WriteByte((byte)MiniMsgpackCode.Raw16);
                stream.WriteByte((byte)(length >> 8));
                stream.WriteByte((byte)(length));
                return 3;
            } else {
                stream.WriteByte((byte)MiniMsgpackCode.Raw32);
                stream.WriteByte((byte)(length >> 24));
                stream.WriteByte((byte)(length >> 16));
                stream.WriteByte((byte)(length >> 8));
                stream.WriteByte((byte)(length));
                return 5;
            }
        }

        public static int PackRaw(Stream stream, Slice data) {
            int num = WriteRawLength(stream, data.Length);
            stream.Write(data.Array, data.Offset, data.Length);
            return num + data.Length;
        }

        public static int PackUInt(Stream stream, ulong num) {
            if (num <= MiniMsgpackCode.MaximumFixedPos) {
                stream.WriteByte((byte)num);
                return 1;
            } else if (num <= 0xFF) {
                stream.WriteByte(MiniMsgpackCode.UnsignedInt8);
                stream.WriteByte((byte)num);
                return 2;
            } else if (num <= 0xFFFF) {
                stream.WriteByte(MiniMsgpackCode.UnsignedInt16);
                stream.WriteByte((byte)(num >> 8));
                stream.WriteByte((byte)num);
                return 3;
            } else if (num <= 0xFFFFFFFF) {
                stream.WriteByte(MiniMsgpackCode.UnsignedInt32);
                stream.WriteByte((byte)(num >> 24));
                stream.WriteByte((byte)(num >> 16));
                stream.WriteByte((byte)(num >> 8));
                stream.WriteByte((byte)num);
                return 5;
            } else {
                stream.WriteByte(MiniMsgpackCode.UnsignedInt64);
                stream.WriteByte((byte)(num >> 56));
                stream.WriteByte((byte)(num >> 48));
                stream.WriteByte((byte)(num >> 40));
                stream.WriteByte((byte)(num >> 32));
                stream.WriteByte((byte)(num >> 24));
                stream.WriteByte((byte)(num >> 16));
                stream.WriteByte((byte)(num >> 8));
                stream.WriteByte((byte)num);
                return 9;
            }
        }

        public static uint UnpackUInt32(Stream stream) {
            int num = 0;

            int flag = stream.ReadByte();
            if (flag <= MiniMsgpackCode.MaximumFixedPos) {
                return (uint)flag;
            } else if (flag == MiniMsgpackCode.UnsignedInt8) {
                return (uint)stream.ReadByte();
            } else if (flag == MiniMsgpackCode.UnsignedInt16) {
                for (int i = 0; i < 2; i++) {
                    num = (num << 8) | (byte)stream.ReadByte();
                }
            } else if (flag == MiniMsgpackCode.UnsignedInt32) {
                for (int i = 0; i < 4; i++) {
                    num = (num << 8) | (byte)stream.ReadByte();
                }
            } else {
                throw new MiniMsgpackException("Unexpected uint flag byte: " + flag);
            }

            return (uint)num;
        }

        public static ulong UnpackUInt64(Stream stream) {
            ulong num = 0;

            int flag = stream.ReadByte();
            if (flag <= MiniMsgpackCode.MaximumFixedPos) {
                return (uint)flag;
            } else if (flag == MiniMsgpackCode.UnsignedInt8) {
                return (uint)stream.ReadByte();
            } else if (flag == MiniMsgpackCode.UnsignedInt16) {
                for (int i = 0; i < 2; i++) {
                    num = (num << 8) | (byte)stream.ReadByte();
                }
            } else if (flag == MiniMsgpackCode.UnsignedInt32) {
                for (int i = 0; i < 4; i++) {
                    num = (num << 8) | (byte)stream.ReadByte();
                }
            } else if (flag == MiniMsgpackCode.UnsignedInt64) {
                for (int i = 0; i < 8; i++) {
                    num = (num << 8) | (byte)stream.ReadByte();
                }
            } else {
                throw new MiniMsgpackException("Unexpected uint flag byte: " + flag);
            }

            return (ulong)num;
        }

        public static int UnpackRawLength(int flag, Stream stream) {
            int length = 0;

            if ((flag & 0xe0) == MiniMsgpackCode.MinimumFixedRaw) {
                length = flag & 0x1f;
            } else if (flag == MiniMsgpackCode.Raw16) {
                for (int i = 0; i < 2; i++) {
                    length = (length << 8) | (byte)stream.ReadByte();
                }
            } else if (flag == MiniMsgpackCode.Raw32) {
                for (int i = 0; i < 4; i++) {
                    length = (length << 8) | (byte)stream.ReadByte();
                }
            } else {
                throw new MiniMsgpackException("Unexpected raw flag byte: " + flag);
            }

            return length;
        }
    }
}

