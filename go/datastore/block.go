package datastore

import (
	"bytes"
	"encoding/binary"
	"sort"
)

// BlockReader provides read access to a single key-value block in a
// tablet.
//
// These have a prefix-encoded key-value section, followed by an index
// of prefix restarts. Each key is preceded by the number of bytes it
// has in common with the previous key:
//
//     [ 0                      key1                          val1 ]
//     [ num_common(key1, key2) key2[num_common(key1, key2):] val2 ]
//     [ num_common(key2, key3) key3[num_common(key2, key3):] val3 ]
//
// The common bytes are encoded as a msgpack uint, and can be positive
// fixnum, uint8, uint16, or uint32.
// http://wiki.msgpack.org/display/MSGPACK/Format+specification#Formatspecification-Integers
//
// Keys and values are encoded as msgpack raw, and can be fix raw, raw
// 16, or raw 32 depending on length.
// http://wiki.msgpack.org/display/MSGPACK/Format+specification#Formatspecification-fixraw
//
// Following this section is an index of the restarts, encoded as
// big-endian 32-bit offsets from the beginning of the block. This can
// be used to search the block.
//
//     [ restart1 ]
//     [ restart2 ]
//     [ restart3 ]
//     [ restart4 ]
//     [ num_restarts ]
//
// Note: these are encoded as fixed 4 byte integers, not msgpack encoded.

type block []byte

// A single iterative view into a block. r must be a reader
// initialized with a byte slice that starts with a restart.
type BlockIterator struct {
	r *bytes.Reader

	// current key and value (during iteration)
	key   []byte
	value []byte

	// previous key
	prevKey []byte
}

func (b block) Find(needle []byte) Iterator {
	numRestarts := int(binary.BigEndian.Uint32(b[len(b)-4:]))
	indexStart := len(b) - 4*numRestarts - 4

	if needle == nil {
		return &BlockIterator{r: bytes.NewReader(b[:indexStart])}
	}

	// get the position in data referred to by the indexed key n
	dataPos := func(n int) uint32 {
		if n == numRestarts {
			return uint32(indexStart)
		} else {
			return binary.BigEndian.Uint32(b[indexStart+4*n:])
		}
	}

	pos := sort.Search(numRestarts, func(i int) bool {
		slice := b[dataPos(i):dataPos(i+1)]

		// Skip the first byte of the slice, which contains
		// the common prefix count. Since this is a restart
		// key, it's guaranteed to be 0x00
		key := peekRaw(slice[1:])
		return bytes.Compare(key, needle) <= 0
	})

	slice := b[dataPos(pos):dataPos(pos+1)]
	iter := BlockIterator{r: bytes.NewReader(slice)}

	// The iterator starts at the restart before needle was
	// found. Advance it until needle.
	iter.Find(needle)

	return &iter
}

func (iter *BlockIterator) Next() bool {
	if iter.r.Len() == 0 {
		iter.Close()
		return false
	}

	common := readUint(iter.r)
	keySuffix := readRaw(iter.r)

	if common > 0 {
		iter.key = append(iter.prevKey[:common], keySuffix...)
	} else {
		iter.key = keySuffix
	}

	iter.value = readRaw(iter.r)
	iter.prevKey = iter.key
	return true
}

func (iter *BlockIterator) Find(needle []byte) {
	iter.r.Seek(0, 0)

	loc := iter.r.Len()
	for iter.Next() && bytes.Compare(iter.Key(), needle) < 0 {
		loc = iter.r.Len()
	}

	// rewind back to the best location
	iter.r.Seek(-int64(loc), 2)
}

func (iter *BlockIterator) Key() []byte {
	return iter.key
}

func (iter *BlockIterator) Value() []byte {
	return iter.value
}

func (iter *BlockIterator) Close() error {
	iter.key = nil
	iter.value = nil
	iter.prevKey = nil
	return nil
}
