package datastore

import (
	"bytes"
	"sort"
)

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Find(key []byte)
	Close() error
}

type SliceIterator struct {
	kvs []*KV
	pos int
}

func NewSliceIterator(kvs []*KV) *SliceIterator {
	return &SliceIterator{kvs, -1}
}

func (iter *SliceIterator) Next() bool {
	iter.pos += 1
	return iter.pos < len(iter.kvs)
}

func (iter *SliceIterator) Find(key []byte) {
	if key == nil {
		iter.pos = -1
		return
	}

	f := func(i int) bool {
		return bytes.Compare(iter.kvs[i].Key, key) >= 0
	}

	// Binary search for the right index, but subtract one because
	// we expect Next() to be called before making the item
	// available
	iter.pos = sort.Search(len(iter.kvs), f) - 1
}

func (iter *SliceIterator) Key() []byte {
	if iter.pos >= len(iter.kvs) {
		return nil
	}
	return iter.kvs[iter.pos].Key
}

func (iter *SliceIterator) Value() []byte {
	if iter.pos >= len(iter.kvs) {
		return nil
	}
	return iter.kvs[iter.pos].Value
}

func (iter *SliceIterator) Close() error {
	return nil
}
