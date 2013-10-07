package datastore

import (
	. "launchpad.net/gocheck"
)

type SliceIteratorSuite struct{}

var _ = Suite(&SliceIteratorSuite{})

func SimpleData() []*KV {
	kvs := []*KV{
		{[]byte("bar"), []byte("baz")},
		{[]byte("foo"), []byte("bar")},
	}

	Sort(kvs)
	return kvs
}

func (s *SliceIteratorSuite) TestSliceIterator(c *C) {
	iter := NewSliceIterator(SimpleData())
	CheckSimpleIterator(c, iter)
}

func (s *SliceIteratorSuite) TestSliceFind(c *C) {
	iter := NewSliceIterator(SimpleData())
	CheckSimpleIterator(c, iter)
}

func CheckSimpleIterator(c *C, iter Iterator) {
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("bar"))
	c.Assert(iter.Value(), DeepEquals, []byte("baz"))

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("foo"))
	c.Assert(iter.Value(), DeepEquals, []byte("bar"))

	c.Assert(iter.Next(), Equals, false)
	c.Assert(iter.Key(), IsNil)
	c.Assert(iter.Value(), IsNil)
}

type SerialIteratorSuite struct{}

var _ = Suite(&SerialIteratorSuite{})

func (s *SerialIteratorSuite) TestSerialIterator(c *C) {
	items1 := []*KV{
		{[]byte("foo"), []byte("foo")},
		{[]byte("bar"), []byte("bar")},
	}

	items2 := []*KV{
		{[]byte("baz"), []byte("baz")},
		{[]byte("quux"), []byte("quux")},
	}

	chained := [][]*KV{items1, items2}

	iter := Chain(len(chained), func(n int) Iterator {
		return NewSliceIterator(chained[n])
	})

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("foo"))
	c.Assert(iter.Value(), DeepEquals, []byte("foo"))

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("bar"))
	c.Assert(iter.Value(), DeepEquals, []byte("bar"))

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("baz"))
	c.Assert(iter.Value(), DeepEquals, []byte("baz"))

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("quux"))
	c.Assert(iter.Value(), DeepEquals, []byte("quux"))

	c.Assert(iter.Next(), Equals, false)
	c.Assert(iter.Key(), IsNil)
	c.Assert(iter.Value(), IsNil)
}

type ParallelIteratorSuite struct{}

var _ = Suite(&ParallelIteratorSuite{})

func (s *ParallelIteratorSuite) TestParallelIterator(c *C) {
	items1 := []*KV{
		{[]byte("bar"), []byte("bar")},
		{[]byte("baz"), []byte("junk")},
		{[]byte("foo"), []byte("foo")},
	}

	items2 := []*KV{
		{[]byte("baz"), []byte("baz")},
		{[]byte("quux"), []byte("quux")},
	}

	iter := Merge(2, func(n int) Iterator {
		if n == 0 {
			return NewSliceIterator(items1)
		} else {
			return NewSliceIterator(items2)
		}
	})

	c.Assert(iter.Next(), Equals, true)
	c.Assert(string(iter.Key()), Equals, "bar")
	c.Assert(string(iter.Value()), Equals, "bar")

	c.Assert(iter.Next(), Equals, true)
	c.Assert(string(iter.Key()), Equals, "baz")
	c.Assert(string(iter.Value()), Equals, "baz")

	c.Assert(iter.Next(), Equals, true)
	c.Assert(string(iter.Key()), Equals, "baz")
	c.Assert(string(iter.Value()), Equals, "junk")

	c.Assert(iter.Next(), Equals, true)
	c.Assert(string(iter.Key()), Equals, "foo")
	c.Assert(string(iter.Value()), Equals, "foo")

	c.Assert(iter.Next(), Equals, true)
	c.Assert(string(iter.Key()), Equals, "quux")
	c.Assert(string(iter.Value()), Equals, "quux")

	c.Assert(iter.Next(), Equals, false)
	c.Assert(iter.Key(), IsNil)
	c.Assert(iter.Value(), IsNil)
}
