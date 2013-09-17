Factory tablet format
=====================

This is largely inspired by the leveldb table format. For now, data blocks are
uncompressed and don't have leveldb's prefix compression or block indexes.

&lt;beginning of file&gt;

[data block 1]

[data block 2]

...

[data block N]

[meta block 1]

...

[meta block K]

[meta index block]

[data index block]

[footer]

&lt;end of file&gt;

Block formats
=============

data block
----------

An uncompressed data block contains a series of key-value pairs, encoded as
msgpack raw bytes:

[ key1 (raw) | value1 (raw) | key2 (raw) | value2 (raw) ... ]

The encoding can be fix raw, raw 16, or raw 32 as appropriate for the key or
value length: [msgpack raw bytes][1]

[1]: <http://wiki.msgpack.org/display/MSGPACK/Format+specification#Formatspecification-Rawbytes>

Items are sorted by key, using lexicographical ordering. Keys and values are
binary safe.

meta block
----------

Metadata block formats TBD. These are user-defined, but we will likely have some
blocks with known names for commonly useful data.

 data index block
-----------------

The data index block contains a magic number, then a series of variable-length
records, one per data block in the file. Longs and ints are serialized as
msgpack uint64 and uint32.

[ 0xda7aba5e ]

[ file offset (uint64) | block length (uint32) | first key (raw) ]

 meta index block
-----------------

The meta index block contains a magic number, then a series of variable-length
records, one per meta block in the file.

[ 0x0ea7da7a ]

[ file offset (uint64) | block length (uint32) | meta block name (raw) ]

 file footer
------------

The file footer is a fixed-length block with pointers to the major sections of
the file, followed by a magic number.

[ meta index offset (uint64) | meta index length (uint64)   data index offset
(uint64) | data index length (uint64) ]

[ 0x0b501e7e ]
