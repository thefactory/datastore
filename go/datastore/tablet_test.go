package datastore

import (
	"io/ioutil"
	. "launchpad.net/gocheck"
	"os"
)

type TabletSuite struct{}

var _ = Suite(&TabletSuite{})

func (s *TabletSuite) TestSimpleEncodeDecode(c *C) {
	kvs := SimpleData()

	CheckEncodeDecode(c, kvs, &TabletOptions{BlockSize: 4096,
		KeyRestartInterval: 10})
	CheckEncodeDecode(c, kvs, &TabletOptions{BlockSize: 1,
		KeyRestartInterval: 10})
	CheckEncodeDecode(c, kvs, &TabletOptions{BlockSize: 4096,
		BlockCompression: Snappy, KeyRestartInterval: 10})
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
		c.Assert(iter.Key(), DeepEquals, kvs[i].Key)
		c.Assert(iter.Value(), DeepEquals, kvs[i].Value)
	}

	// ensure all the expected elements were checked
	c.Check(i, Equals, len(kvs))

	os.Remove(file.Name())
}

func (s *TabletSuite) TestTabletIterator(c *C) {
	file, err := ioutil.TempFile("", "tablet_test")
	c.Check(err, IsNil)

	opts := &TabletOptions{BlockSize: 4096, KeyRestartInterval: 10}
	WriteTablet(file, NewSliceIterator(SimpleData()), opts)
	file.Close()

	tab, err := OpenTabletFile(file.Name())
	c.Check(err, IsNil)

	CheckSimpleIterator(c, tab.Iterator())
}

func (s *TabletSuite) TestTabletFind(c *C) {
	file, err := ioutil.TempFile("", "tablet_test")
	c.Check(err, IsNil)

	opts := &TabletOptions{BlockSize: 4096, KeyRestartInterval: 10}
	WriteTablet(file, NewSliceIterator(SimpleData()), opts)
	file.Close()

	tab, err := OpenTabletFile(file.Name())
	c.Check(err, IsNil)

	CheckSimpleFind(c, tab.Iterator())
}
