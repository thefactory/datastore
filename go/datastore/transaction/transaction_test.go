package transaction

import (
	"bytes"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"os"
)

type WriterSuite struct{}

var _ = Suite(&WriterSuite{})

func cleanup(file *os.File) {
	file.Close()
	os.Remove(file.Name())
}

func (s *WriterSuite) TestSmallWrites(c *C) {
	file, _ := ioutil.TempFile("", "unittest")
	defer cleanup(file)

	w, err := NewFileWriter(file.Name())
	c.Assert(err, IsNil)

	w.Write([]byte("foo"))
	w.Write([]byte("bar"))
	w.Write([]byte("baz"))

	data, err := ioutil.ReadFile(file.Name())
	c.Assert(err, IsNil)

	c.Assert(data, DeepEquals, []byte{
		// "foo"
		0x8c, 0x73, 0x65, 0x21, // checksum
		0x1,      // type: full record
		0x0, 0x3, // length
		'f', 'o', 'o',

		// "bar"
		0x76, 0xff, 0x8c, 0xaa,
		0x1,
		0x0, 0x3,
		'b', 'a', 'r',

		// "baz"
		0x78, 0x24, 0x4, 0x98,
		0x1,
		0x0, 0x3,
		'b', 'a', 'z',
	})
}

func (s *WriterSuite) TestTwoRecordWrite(c *C) {
	file, _ := ioutil.TempFile("", "unittest")
	defer cleanup(file)

	w, err := NewFileWriter(file.Name())
	c.Assert(err, IsNil)

	// write data that's larger than one block (after header)
	rec := bytes.Repeat([]byte{0xee}, blockSize)
	w.Write(rec)

	data, err := ioutil.ReadFile(file.Name())
	c.Assert(err, IsNil)

	header := data[:headerLen]
	c.Assert(header, DeepEquals, []byte{
		0xaa, 0xcc, 0x7b, 0xb3, // checksum
		0x2,        // type: first record
		0x7f, 0xf9, // length: 32768-7 = 32761
	})

	// verify that bytes [7:] are the first N bytes of the record
	c.Assert(data[headerLen:blockSize], DeepEquals,
		rec[:blockSize-headerLen])

	// check the header for the next block
	header = data[blockSize : blockSize+headerLen]
	c.Assert(header, DeepEquals, []byte{
		0x67, 0xca, 0xdb, 0xc4, // checksum
		0x4,      // type: last record
		0x0, 0x7, // length: 7
	})
}

func (s *WriterSuite) TestThreeRecordWrite(c *C) {
	file, _ := ioutil.TempFile("", "unittest")
	defer cleanup(file)

	w, err := NewFileWriter(file.Name())
	c.Assert(err, IsNil)

	// write data that's larger than one block (after header)
	rec := bytes.Repeat([]byte{0xee}, blockSize*2)
	w.Write(rec)

	data, err := ioutil.ReadFile(file.Name())
	c.Assert(err, IsNil)

	header := data[:headerLen]
	c.Assert(header, DeepEquals, []byte{
		0xaa, 0xcc, 0x7b, 0xb3, // checksum
		0x2,        // type: first record
		0x7f, 0xf9, // length: 32768-7 = 32761
	})

	// check the header for the next block
	header = data[1*blockSize : 1*blockSize+headerLen]
	c.Assert(header, DeepEquals, []byte{
		0xaa, 0xcc, 0x7b, 0xb3, // checksum
		0x3,        // type: middle record
		0x7f, 0xf9, // length: 32761
	})

	// check the header for the last block
	header = data[2*blockSize : 2*blockSize+headerLen]
	c.Assert(header, DeepEquals, []byte{
		0x22, 0x35, 0xb3, 0x70, // checksum
		0x4,      // type: last record
		0x0, 0xe, // length: 14
	})
}

func (s *WriterSuite) TestRecordPadding(c *C) {
	file, _ := ioutil.TempFile("", "unittest")
	defer cleanup(file)

	w, err := NewFileWriter(file.Name())
	c.Assert(err, IsNil)

	// write data that leaves 6 bytes empty at the end of a block,
	// then again so the first block is closed
	rec := bytes.Repeat([]byte{0xee}, blockSize-headerLen-6)
	w.Write(rec)
	w.Write(rec)

	data, err := ioutil.ReadFile(file.Name())
	c.Assert(err, IsNil)

	padding := data[blockSize-6 : blockSize]
	c.Assert(padding, DeepEquals, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0})
}

func (s *WriterSuite) TestZeroLengthFirstRecord(c *C) {
	file, _ := ioutil.TempFile("", "unittest")
	defer cleanup(file)

	w, err := NewFileWriter(file.Name())
	c.Assert(err, IsNil)

	// write data that leaves 7 bytes empty at the end of a block
	rec := bytes.Repeat([]byte{0xee}, blockSize-headerLen-7)
	w.Write(rec)
	w.Write(rec)

	data, err := ioutil.ReadFile(file.Name())
	c.Assert(err, IsNil)

	// the end of the first block should have a FIRST header with
	// zero length
	padding := data[blockSize-7 : blockSize]
	c.Assert(padding, DeepEquals, []byte{
		0x0, 0x0, 0x0, 0x0, // checksum
		0x2,      // record type: first
		0x0, 0x0, // length
	})

	// and the next block should start with a LAST containing the data
	header := data[blockSize : blockSize+headerLen]
	c.Assert(header, DeepEquals, []byte{
		0x67, 0x28, 0x75, 0x11, // checksum
		0x4,        // record type: last
		0x7f, 0xf2, // length
	})
}

func (s *WriterSuite) TestZeroLengthFullRecord(c *C) {
	file, _ := ioutil.TempFile("", "unittest")
	defer cleanup(file)

	w, err := NewFileWriter(file.Name())
	c.Assert(err, IsNil)

	// write data that leaves 7 bytes empty at the end of a block
	rec := bytes.Repeat([]byte{0xee}, blockSize-headerLen-7)
	w.Write(rec)

	// write a zero-length record
	w.Write([]byte{})

	data, err := ioutil.ReadFile(file.Name())
	c.Assert(err, IsNil)

	// the end of the first block should have a FULL header with
	// zero length
	padding := data[blockSize-7 : blockSize]
	c.Assert(padding, DeepEquals, []byte{
		0x0, 0x0, 0x0, 0x0, // checksum
		0x1,      // record type: full
		0x0, 0x0, // length
	})
}
