package datastore

import (
	"bytes"
	"sort"
)

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
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

// chain together n iterators, loaded lazily in order
func Chain(n int, f func(int) Iterator) Iterator {
	return &SerialIterator{n, f, 0, nil}
}

type SerialIterator struct {
	n int
	f func(n int) Iterator

	cur  int
	iter Iterator
}

func (si *SerialIterator) Next() bool {
	if si.iter == nil {
		if si.done() {
			return false
		}

		// switch to next iterator
		si.iter = si.f(si.cur)
	}

	next := si.iter.Next()
	if next {
		// current iterator still has values
		return true
	}

	si.cur++
	si.iter = nil

	if si.done() {
		return false
	}

	si.iter = si.f(si.cur)
	return si.iter.Next()
}

func (si *SerialIterator) done() bool {
	return si.cur >= si.n
}

func (si *SerialIterator) Key() []byte {
	if si.done() {
		return nil
	}

	return si.iter.Key()
}

func (si *SerialIterator) Value() []byte {
	if si.done() {
		return nil
	}

	return si.iter.Value()
}

func (si *SerialIterator) Close() error {
	if si.done() {
		return nil
	}

	ret := si.iter.Close()

	si.f = nil
	si.cur = si.n + 1
	si.iter = nil

	return ret
}
