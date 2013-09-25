package datastore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
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
	magic         uint32
	blockEncoding BlockEncodingType
	future1       uint8
	future2       uint8
	future3       uint8
}

type Footer struct {
	metaOffset uint64
	metaLength uint64
	dataOffset uint64
	dataLength uint64
	magic      uint32
}

type BlockEncodingType uint8

const (
	// msgpack-encoded key-value pairs, raw(key1) raw(val1) raw(key2) ...
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

	metaBlocks, _ := readIndex(r, metaIndexMagic, footer.metaOffset, footer.metaLength)
	dataBlocks, _ := readIndex(r, dataIndexMagic, footer.dataOffset, footer.dataLength)

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
		flags[1], flags[2], flags[3]}

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
		return nil, errors.New("unexpected magic number in index")
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

func readUint32(r io.Reader) uint32 {
	var junk byte
	var ret uint32

	binary.Read(r, binary.BigEndian, &junk)
	binary.Read(r, binary.BigEndian, &ret)
	return ret
}

func readUint64(r io.Reader) uint64 {
	var junk byte
	var ret uint64

	binary.Read(r, binary.BigEndian, &junk)
	binary.Read(r, binary.BigEndian, &ret)
	return ret
}

func readRaw(r io.Reader) []byte {
	var flag byte
	binary.Read(r, binary.BigEndian, &flag)

	var length uint32
	if flag&msgFixRaw == msgFixRaw {
		length = uint32(flag & (^msgFixRaw))
	} else if flag == msgRaw16 {
		var tmp uint16
		binary.Read(r, binary.BigEndian, &tmp)
		length = uint32(tmp)
	} else if flag == msgRaw32 {
		binary.Read(r, binary.BigEndian, &length)
	} else {
		log.Printf("ERROR: bad flag in readRaw\n", flag)
	}

	buf := make([]byte, length)
	r.Read(buf)
	return buf
}

type RawBlockIterator struct {
	buf   *bytes.Reader
	key   []byte
	value []byte
}

func NewRawBlockIterator(data []byte) *RawBlockIterator {
	return &RawBlockIterator{buf: bytes.NewReader(data)}
}

func (iter *RawBlockIterator) Next() bool {
	if iter.buf.Len() == 0 {
		iter.key = nil
		iter.value = nil
		return false
	}

	iter.key = readRaw(iter.buf)
	iter.value = readRaw(iter.buf)

	return true
}

func (iter *RawBlockIterator) Find(key []byte) {
	iter.buf.Seek(0, 0)

	if key == nil {
		return
	}

	loc := iter.buf.Len()
	for iter.Next() && bytes.Compare(iter.Key(), key) < 0 {
		// a raw block reader can't do better than linear search
		loc = iter.buf.Len()
	}

	// rewind back to the best location
	iter.buf.Seek(-int64(loc), 2)
}

func (iter *RawBlockIterator) Key() []byte {
	return iter.key
}

func (iter *RawBlockIterator) Value() []byte {
	return iter.value
}

func (iter *RawBlockIterator) Close() error {
	return nil
}

// A key-value iterator for a single block
func (t *Tablet) BlockIterator(rec *IndexRecord) Iterator {
	buf := make([]byte, rec.length)

	t.r.ReadAt(buf, int64(rec.offset))

	return NewRawBlockIterator(buf)
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
