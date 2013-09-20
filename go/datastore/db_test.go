package datastore

import (
	"bytes"
	"testing"
)

func Test_SliceIterator(t *testing.T) {
	kvs := []*KV{
		{[]byte("foo"), []byte("bar")},
		{[]byte("bar"), []byte("baz")},
	}

	expected := [][]byte{[]byte("foo"), []byte("bar")}

	iter := NewSliceIterator(kvs)

	for _, key := range expected {
		if !iter.Next() {
			t.Error("Early end of iterator")
		}

		if bytes.Compare(iter.Key(), key) != 0 {
			t.Error("Bad key from iterator: ", string(key))
		}
	}

	if iter.Next() {
		t.Error("Iterator should be finished")
	}
}
