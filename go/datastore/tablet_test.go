package datastore

import (
	"io/ioutil"
	. "launchpad.net/gocheck"
	"os"
)

type TabletSuite struct{}

var _ = Suite(&TabletSuite{})

func (s *TabletSuite) TestRawBlockIterator(c *C) {
	// bar -> baz, foo -> bar
	block := []byte{
		0xa3, 'b', 'a', 'r', 0xa3, 'b', 'a', 'z',
		0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
	}

	iter := NewRawBlockIterator(block)
	CheckSimpleIterator(c, iter)
}

func (s *TabletSuite) TestRawBlockFind(c *C) {
	// bar -> baz, foo -> bar
	block := []byte{
		0xa3, 'b', 'a', 'r', 0xa3, 'b', 'a', 'z',
		0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
	}

	iter := NewRawBlockIterator(block)
	CheckSimpleFind(c, iter)
}

func (s *TabletSuite) TestSimpleEncodeDecode(c *C) {
	kvs := []*KV{
		{[]byte("bar"), []byte("baz")},
		{[]byte("foo"), []byte("bar")},
	}

	CheckEncodeDecode(c, kvs, &TabletOptions{BlockSize: 4096})
}

func CheckEncodeDecode(c *C, kvs []*KV, opts *TabletOptions) {
	file, err := ioutil.TempFile("", "tablet_test")
	c.Check(err, IsNil)

	WriteTablet(file, NewSliceIterator(kvs), opts)
	file.Close()

	tab, err := OpenTabletFile(file.Name())
	c.Check(err, IsNil)

	iter := tab.Iterator()

	var i int
	for i = 0; iter.Next(); i++ {
		c.Check(iter.Key(), DeepEquals, kvs[i].Key)
		c.Check(iter.Value(), DeepEquals, kvs[i].Value)
	}

	// ensure all the expected elements were checked
	c.Check(i, Equals, len(kvs))

	os.Remove(file.Name())
}
