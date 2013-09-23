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

func CheckSimpleFind(c *C, iter Iterator) {
	// seek to nil
	iter.Find(nil)
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("bar"))

	// seek before the first element
	iter.Find([]byte("ba"))
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("bar"))

	// seek to the actual first element
	iter.Find([]byte("bar"))
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("bar"))

	// seek to the actual second element
	iter.Find([]byte("foo"))
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("foo"))

	// seek beyond the last element
	iter.Find([]byte("fooo"))
	c.Assert(iter.Next(), Equals, false)
}
