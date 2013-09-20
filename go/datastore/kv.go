package datastore

import (
	"bytes"
	"sort"
)

type KV struct {
	Key   []byte
	Value []byte
}

type KVs []*KV

func (kvs KVs) Len() int      { return len(kvs) }
func (kvs KVs) Swap(i, j int) { kvs[i], kvs[j] = kvs[j], kvs[i] }

type ByBytes struct{ KVs }

func (kvs ByBytes) Less(i, j int) bool {
	return bytes.Compare(kvs.KVs[i].Key, kvs.KVs[j].Key) < 0
}

func Sort(kvs []*KV) {
	sort.Sort(ByBytes{kvs})
}
