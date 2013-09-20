package datastore

import (
	"bytes"
	"sort"
)

type KV struct {
	Key   []byte
	Value []byte
}

func Sort(kvs []KV) {
	bytes := func(kv1, kv2 *KV) bool {
		return bytes.Compare(kv1.Key, kv2.Key) < 0
	}

	By(bytes).Sort(kvs)
}

type By func(kv1, kv2 *KV) bool

func (by By) Sort(kvs []KV) {
	sorter := &kvSorter{kvs: kvs, by: by}
	sort.Sort(sorter)
}

type kvSorter struct {
	kvs []KV
	by  func(kv1, kv2 *KV) bool
}

func (s *kvSorter) Len() int {
	return len(s.kvs)
}

func (s *kvSorter) Swap(i, j int) {
	s.kvs[i], s.kvs[j] = s.kvs[j], s.kvs[i]
}

func (s *kvSorter) Less(i, j int) bool {
	return s.by(&s.kvs[i], &s.kvs[j])
}
