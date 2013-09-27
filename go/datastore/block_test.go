package datastore

import (
	"bytes"
	. "launchpad.net/gocheck"
)

type BlockReaderSuite struct{}

var _ = Suite(&BlockReaderSuite{})

func (s *BlockReaderSuite) TestEmptyBlock(c *C) {
	b := []byte{
		// no kv pairs
		// no index
		0x00, 0x00, 0x00, 0x00,
	}

	r := block(b).Find(nil)
	c.Assert(r.Next(), Equals, false)
}

func (s *BlockReaderSuite) TestOneKv(c *C) {
	b := []byte{
		// foo -> bar
		0x00, 0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
		// restarts index (one restart, uint32 block[0])
		0x00, 0x00, 0x00, 0x00,
		// one restart
		0x00, 0x00, 0x00, 0x01,
	}

	r := block(b).Find(nil)

	c.Assert(r.Next(), Equals, true)
	c.Assert(r.Key(), DeepEquals, []byte("foo"))
	c.Assert(r.Value(), DeepEquals, []byte("bar"))

	c.Assert(r.Next(), Equals, false)
}

func (s *BlockReaderSuite) TestTwoKvs(c *C) {
	b := []byte{
		// foo -> bar, food -> baz
		0x00, 0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
		0x03, 0xa1, 'd', 0xa3, 'b', 'a', 'z',
		// restarts index (one restart, uint32 block[0])
		0x00, 0x00, 0x00, 0x00,
		// one restart
		0x00, 0x00, 0x00, 0x01,
	}

	r := block(b).Find(nil)

	c.Assert(r.Next(), Equals, true)
	c.Assert(r.Key(), DeepEquals, []byte("foo"))
	c.Assert(r.Value(), DeepEquals, []byte("bar"))

	c.Assert(r.Next(), Equals, true)
	c.Assert(r.Key(), DeepEquals, []byte("food"))
	c.Assert(r.Value(), DeepEquals, []byte("baz"))

	c.Assert(r.Next(), Equals, false)
}

func (s *BlockReaderSuite) TestTwoRestarts(c *C) {
	b := []byte{
		// foo -> bar, food -> baz, two -> x
		0x00, 0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
		0x03, 0xa1, 'd', 0xa3, 'b', 'a', 'z',
		0x00, 0xa3, 't', 'w', 'o', 0xa1, 'x',
		// restarts index (two restarts, block[0] and block[16])
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x10,
		// two restarts
		0x00, 0x00, 0x00, 0x02,
	}

	r := block(b).Find(nil)

	c.Assert(r.Next(), Equals, true)
	c.Assert(r.Key(), DeepEquals, []byte("foo"))
	c.Assert(r.Value(), DeepEquals, []byte("bar"))

	c.Assert(r.Next(), Equals, true)
	c.Assert(r.Key(), DeepEquals, []byte("food"))
	c.Assert(r.Value(), DeepEquals, []byte("baz"))

	c.Assert(r.Next(), Equals, true)
	c.Assert(r.Key(), DeepEquals, []byte("two"))
	c.Assert(r.Value(), DeepEquals, []byte("x"))

	c.Assert(r.Next(), Equals, false)
}

type BlockIteratorSuite struct{}

var _ = Suite(&BlockIteratorSuite{})

func (s *BlockIteratorSuite) TestNext(c *C) {
	kvs := []byte{
		0x00, 0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
	}

	iter := BlockIterator{r: bytes.NewReader(kvs)}

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("foo"))
	c.Assert(iter.Value(), DeepEquals, []byte("bar"))

	c.Assert(iter.Next(), Equals, false)
}

func (s *BlockIteratorSuite) TestNextCompressed(c *C) {
	kvs := []byte{
		0x00, 0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
		0x03, 0xa3, 'o', 'o', 'o', 0xa3, 'b', 'a', 'r',
	}

	iter := BlockIterator{r: bytes.NewReader(kvs)}

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("foo"))
	c.Assert(iter.Value(), DeepEquals, []byte("bar"))

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("fooooo"))
	c.Assert(iter.Value(), DeepEquals, []byte("bar"))

	c.Assert(iter.Next(), Equals, false)
}

func (s *BlockIteratorSuite) TestNextFind(c *C) {
	kvs := []byte{
		0x00, 0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
		0x03, 0xa3, 'o', 'o', 'o', 0xa3, 'b', 'a', 'r',
	}

	iter := BlockIterator{r: bytes.NewReader(kvs)}
	iter.Find([]byte("fooooo"))

	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("fooooo"))
	c.Assert(iter.Value(), DeepEquals, []byte("bar"))

	c.Assert(iter.Next(), Equals, false)
}

func (s *BlockReaderSuite) TestFind(c *C) {
	b := []byte{
		// foo -> bar, food -> baz, two -> x
		0x00, 0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
		0x03, 0xa1, 'd', 0xa3, 'b', 'a', 'z',
		0x00, 0xa3, 't', 'w', 'o', 0xa1, 'x',
		// restarts index (two restarts, block[0] and block[16])
		0x00, 0x00, 0x00, 0x00,
		// one restart
		0x00, 0x00, 0x00, 0x01,
	}

	r := block(b)

	var iter Iterator

	iter = r.Find(nil)
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("foo"))

	iter = r.Find([]byte("foo"))
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("foo"))

	iter = r.Find([]byte("food"))
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("food"))

	iter = r.Find([]byte("two"))
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("two"))
	c.Assert(iter.Next(), Equals, false)

	iter = r.Find([]byte("twoo"))
	c.Assert(iter.Next(), Equals, false)
}
