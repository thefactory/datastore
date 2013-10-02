Factory tablet format
=====================

This is largely inspired by the leveldb table format. For now, data blocks are
uncompressed and don't have leveldb's prefix compression or block indexes.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
--beginning of file--
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
--end of file--
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Block formats
=============

data block
----------

An uncompressed data block contains a series of key-value pairs,
encoded as msgpack raw bytes.

It contains a prefix-encoded key-value section followed by a list of
prefix restarts. Each key is preceded by the number of bytes it has in
common with the previous key:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      # common bytes         key                           value
    --------------------------------------------------------------
    [ 0                      key1                          val1  ]
    [ num_common(key1, key2) key2[num_common(key1, key2):] val2  ]
    [ num_common(key2, key3) key3[num_common(key2, key3):] val3  ]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The common bytes are encoded as a msgpack uint, and can be positive
fixnum, uint8, uint16, or uint32.

http://wiki.msgpack.org/display/MSGPACK/Format+specification#Formatspecification-Integers

Keys and values are encoded as msgpack raw, and can be fix raw, raw
16, or raw 32 depending on length.
http://wiki.msgpack.org/display/MSGPACK/Format+specification#Formatspecification-fixraw

Following this section is an index of the restarts (positions with 0
common bytes), encoded as big-endian 32-bit offsets from the beginning
of the block. This can be used to search the block.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    [ restart1 ]
    [ restart2 ]
    [ restart3 ]
    [ restart4 ]
    [ num_restarts ]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Note: these are encoded as fixed 4 byte integers, not msgpack encoded.

Items are sorted by key, using lexicographical ordering. Keys and values are
binary safe. Keys can be tombstoned with a msgpack Nil byte as their value
(0xc0).

meta block
----------

Metadata blocks are formatted the same as data blocks, but they're
used for higher level organization by a tablet-using application.

 data index block
-----------------

The data index block contains a magic number, then a series of variable-length
records, one per data block in the file. Longs and ints are serialized as
msgpack uint64 and uint32.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[ 0xda7aba5e ]
[ block 1: file offset (uint64) | block length (uint32) | first key (raw) ]
[ block 2: file offset (uint64) | block length (uint32) | first key (raw) ]
...
[ block N: file offset (uint64) | block length (uint32) | first key (raw) ]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 meta index block
-----------------

The meta index block contains a magic number, then a series of variable-length
records, one per meta block in the file.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[ 0x0ea7da7a ]
[ meta 1: file offset (uint64) | block length (uint32) | meta block name (raw) ]
[ meta 2: file offset (uint64) | block length (uint32) | meta block name (raw) ]
...
[ meta N: file offset (uint64) | block length (uint32) | meta block name (raw) ]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 file footer
------------

The file footer is a fixed-length block with pointers to the major sections of
the file, followed by a magic number.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[ meta index offset (uint64) | meta index length (uint64) ]
[ data index offset (uint64) | data index length (uint64) ]
[ 0x0b501e7e ]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
