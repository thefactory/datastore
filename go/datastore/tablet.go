package datastore

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

type TabletFile interface {
	io.ReadSeeker
	ReadAt(p []byte, off int64) (int, error)
}

type Tablet struct {
	r          TabletFile
	header     *Header
	metaBlocks []*IndexRecord
	dataBlocks []*IndexRecord
}

type Header struct {
	magic            uint32
	blockEncoding    BlockEncodingType
	blockCompression BlockCompressionType
	future1          uint8
	future2          uint8
}

type Footer struct {
	metaOffset uint64
	metaLength uint64
	dataOffset uint64
	dataLength uint64
	magic      uint32
}

type BlockCompressionType uint8

const (
	None BlockCompressionType = iota
	Snappy
)

type BlockEncodingType uint8

const (
	Raw BlockEncodingType = iota
)

func OpenTablet(r TabletFile) (*Tablet, error) {
	header, err := readHeader(r)
	if err != nil {
		return nil, err
	}

	if header.blockEncoding > Raw {
		msg := fmt.Sprintf("unsupported block encoding: 0x%x",
			header.blockEncoding)
		return nil, errors.New(msg)
	}

	footer, err := readFooter(r)
	if err != nil {
		return nil, err
	}

	metaBlocks, err := readIndex(r, metaIndexMagic, footer.metaOffset, footer.metaLength)
	if err != nil {
		return nil, err
	}

	dataBlocks, err := readIndex(r, dataIndexMagic, footer.dataOffset, footer.dataLength)
	if err != nil {
		return nil, err
	}

	return &Tablet{r, header, metaBlocks, dataBlocks}, nil
}

func OpenTabletFile(path string) (*Tablet, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return OpenTablet(fd)
}

func readHeader(r io.Reader) (*Header, error) {
	var magic uint32
	var flags [4]byte

	err := binary.Read(r, binary.BigEndian, &magic)
	if err != nil {
		return nil, err
	}

	err = binary.Read(r, binary.BigEndian, &flags)
	if err != nil {
		return nil, err
	}

	h := Header{magic, BlockEncodingType(flags[0]),
		BlockCompressionType(flags[1]), flags[2], flags[3]}

	err = validateHeader(&h)
	if err != nil {
		return nil, err
	}

	return &h, nil
}

func validateHeader(h *Header) error {
	if h.magic != tabletMagic {
		return errors.New("bad magic number in header")
	}

	if h.blockEncoding > Raw {
		msg := fmt.Sprintf("unknown block encoding: 0x%x",
			h.blockEncoding)
		return errors.New(msg)
	}

	if h.blockCompression > Snappy {
		msg := fmt.Sprintf("unknown block compression: 0x%x",
			h.blockCompression)
		return errors.New(msg)
	}

	return nil
}

func readFooter(r TabletFile) (*Footer, error) {
	var metaOffset, metaLength, dataOffset, dataLength uint64
	var magic uint32

	r.Seek(-40, 2)
	metaOffset = readUint64(r)
	metaLength = readUint64(r)
	dataOffset = readUint64(r)
	dataLength = readUint64(r)

	err := binary.Read(r, binary.BigEndian, &magic)
	if err != nil {
		return nil, err
	}

	if magic != tabletMagic {
		return nil, errors.New("bad magic number in footer")
	}

	return &Footer{metaOffset, metaLength, dataOffset, dataLength, magic}, nil
}

func readIndex(r TabletFile, magic uint32, offset uint64, length uint64) ([]*IndexRecord, error) {
	buf := make([]byte, length)

	_, err := r.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}

	br := bytes.NewReader(buf)

	var head uint32
	binary.Read(br, binary.BigEndian, &head)
	if head != magic {
		return nil, errors.New(fmt.Sprintf("unexpected magic number in index: %x (wanted %x)", head, magic))
	}

	return readIndexRecords(br), nil
}

func readIndexRecords(b *bytes.Reader) []*IndexRecord {
	ret := make([]*IndexRecord, 0)

	for b.Len() > 0 {
		offset := readUint64(b)
		length := readUint32(b)
		name := readRaw(b)

		ret = append(ret, &IndexRecord{offset, length, name})
	}

	return ret
}

func loadBlock(tf TabletFile, rec *IndexRecord) (block, error) {
	buf := make([]byte, rec.length)
	tf.ReadAt(buf, int64(rec.offset))

	// grab the block checksum, compression type, and length
	r := bytes.NewReader(buf)
	readUint(r)
	blockType := BlockCompressionType(readUint(r))
	length := readUint(r)

	// grab the unread bytes in buf as the block data
	data := buf[len(buf)-int(length):]

	var b block
	var err error

	if blockType == Snappy {
		tmp, err0 := snappy.Decode(nil, data)
		b = block(tmp)
		err = err0
	} else {
		b = block(data)
	}

	return b, err
}

// A key-value iterator for a single block
func (t *Tablet) BlockIterator(rec *IndexRecord) Iterator {
	b, _ := loadBlock(t.r, rec)

	return block(b).Find(nil)
}

func (t *Tablet) Iterator() Iterator {
	return &TabletIterator{tab: t}
}

type TabletIterator struct {
	tab  *Tablet
	cur  int
	iter Iterator
	eoi  bool
}

func (ti *TabletIterator) Next() bool {
	if ti.iter == nil {
		ti.setBlock(0)
	}

	if ti.eoi {
		return false
	}

	if ti.iter.Next() == false {
		ti.setBlock(ti.cur + 1)
		return ti.iter.Next()
	}

	return true
}

func (ti *TabletIterator) setBlock(n int) {
	if ti.iter != nil {
		ti.iter.Close()
	}

	if n < len(ti.tab.dataBlocks) {
		ti.cur = n
		ti.iter = ti.tab.BlockIterator(ti.tab.dataBlocks[n])
	} else {
		ti.eoi = true
	}
}

func (ti *TabletIterator) Find(key []byte) {
	f := func(i int) bool {
		return bytes.Compare(ti.tab.dataBlocks[i].name, key) >= 0
	}

	ti.setBlock(sort.Search(len(ti.tab.dataBlocks)-1, f))
	ti.iter.Find(key)
}

func (ti *TabletIterator) Key() []byte {
	return ti.iter.Key()
}

func (ti *TabletIterator) Value() []byte {
	return ti.iter.Value()
}

func (ti *TabletIterator) Close() error {
	ti.setBlock(len(ti.tab.dataBlocks))
	return nil
}
