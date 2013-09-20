package datastore

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

func (iter *SliceIterator) Key() []byte {
	return iter.kvs[iter.pos].Key
}

func (iter *SliceIterator) Value() []byte {
	return iter.kvs[iter.pos].Value
}

func (iter *SliceIterator) Close() error {
	return nil
}
