package datastore

import (
	. "launchpad.net/gocheck"
)

type BlockWriterSuite struct{}

var _ = Suite(&BlockWriterSuite{})

func (s *BlockWriterSuite) TestEmptyBlock(c *C) {
	w := NewBlockWriter(&TabletOptions{BlockSize: 4096, KeyRestartInterval: 10})

	_, buf := w.Finish()

	c.Assert(buf, DeepEquals, []byte{
		// zero restarts
		0x00, 0x00, 0x00, 0x00,
	})
}

func (s *BlockWriterSuite) TestOneKv(c *C) {
	w := NewBlockWriter(&TabletOptions{BlockSize: 4096, KeyRestartInterval: 10})

	w.Append([]byte("baz"), []byte("quux"))
	w.Append([]byte("foo"), []byte("bar"))

	firstKey, buf := w.Finish()

	c.Assert(firstKey, DeepEquals, []byte("baz"))

	c.Assert(buf, DeepEquals, []byte{
		// baz -> quux
		0x00, 0xa3, 'b', 'a', 'z', 0xa4, 'q', 'u', 'u', 'x',
		// foo -> bar
		0x00, 0xa3, 'f', 'o', 'o', 0xa3, 'b', 'a', 'r',
		// restarts index (one restart, uint32 block[0])
		0x00, 0x00, 0x00, 0x00,
		// one restart
		0x00, 0x00, 0x00, 0x01,
	})
}

func (s *BlockWriterSuite) TestPrefixCompression(c *C) {
	w := NewBlockWriter(&TabletOptions{BlockSize: 4096, KeyRestartInterval: 10})

	w.Append([]byte("baz"), []byte("quux"))
	w.Append([]byte("bazz"), []byte("quuux"))

	firstKey, buf := w.Finish()

	c.Assert(firstKey, DeepEquals, []byte("baz"))

	c.Assert(buf, DeepEquals, []byte{
		// baz -> quux
		0x00, 0xa3, 'b', 'a', 'z', 0xa4, 'q', 'u', 'u', 'x',
		// bazz -> quuux
		0x03, 0xa1, 'z', 0xa5, 'q', 'u', 'u', 'u', 'x',
		// restarts index (one restart, uint32 block[0])
		0x00, 0x00, 0x00, 0x00,
		// one restart
		0x00, 0x00, 0x00, 0x01,
	})
}

func (s *BlockWriterSuite) TestTwoRestarts(c *C) {
	w := NewBlockWriter(&TabletOptions{BlockSize: 4096, KeyRestartInterval: 1})

	w.Append([]byte("baz"), []byte("quux"))
	w.Append([]byte("bazz"), []byte("quuux"))

	firstKey, buf := w.Finish()

	c.Assert(firstKey, DeepEquals, []byte("baz"))

	c.Assert(buf, DeepEquals, []byte{
		// baz -> quux
		0x00, 0xa3, 'b', 'a', 'z', 0xa4, 'q', 'u', 'u', 'x',
		// bazz -> quuux
		0x00, 0xa4, 'b', 'a', 'z', 'z', 0xa5, 'q', 'u', 'u', 'u', 'x',
		// restarts index (two restarts, uint32 block[0] and block[10])
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x0a,
		// two restarts
		0x00, 0x00, 0x00, 0x02,
	})
}
