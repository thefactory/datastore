package datastore

import (
	"bytes"
	"container/heap"
	"log"
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

// Merge together n in-order iterators. When duplicate keys are
// presented on several iterators, use the one with the highest index.
func Merge(n int, f func(int) Iterator) Iterator {
	keyQueue := &keyQueue{}
	heap.Init(keyQueue)

	iters := make([]Iterator, 0, n)

	// fill the queue with the first item from each iterator
	for i := 0; i < n; i++ {
		iter := f(i)
		if iter.Next() {
			heap.Push(keyQueue, &priorityKey{-i, iter.Key()})
		}
		iters = append(iters, iter)
	}

	return &ParallelIterator{iters, keyQueue, nil, nil}
}

type ParallelIterator struct {
	iters []Iterator
	keys  *keyQueue

	key   []byte
	value []byte
}

func (pi *ParallelIterator) Next() bool {
	if len(*pi.keys) == 0 {
		pi.key = nil
		pi.value = nil
		return false
	}

	item := heap.Pop(pi.keys).(*priorityKey)
	iter := pi.iters[-item.priority]

	pi.key = iter.Key()
	pi.value = iter.Value()

	if iter.Next() {
		heap.Push(pi.keys, &priorityKey{item.priority, iter.Key()})
	}

	return true
}

func (pi *ParallelIterator) Key() []byte {
	return pi.key
}

func (pi *ParallelIterator) Value() []byte {
	return pi.value
}

func (pi *ParallelIterator) Close() error {
	for _, iter := range pi.iters {
		iter.Close()
	}

	return nil
}

// this is a key with a priority; minimum priority wins
type priorityKey struct {
	priority int
	key      []byte
}

type keyQueue []*priorityKey

func (q keyQueue) Len() int {
	return len(q)
}

func (q keyQueue) Less(i, j int) bool {
	cmp := bytes.Compare(q[i].key, q[j].key)
	if cmp < 0 {
		return true
	}

	if cmp == 0 {
		return q[i].priority < q[j].priority
	}

	return false
}

func (q keyQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *keyQueue) Push(x interface{}) {
	*q = append(*q, x.(*priorityKey))
}

func (q *keyQueue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

func (q *keyQueue) dump() {
	for i := 0; i < len(*q); i++ {
		log.Printf("Queue[%n] %s", i, (*q)[i])
	}
}
