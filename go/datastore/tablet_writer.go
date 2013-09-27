package datastore

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"encoding/binary"
	"errors"
	"io"
	"log"
)

const (
	tabletMagic    uint32 = 0x0b501e7e
	metaIndexMagic uint32 = 0x0ea7da7a
	dataIndexMagic uint32 = 0xda7aba5e

	msgFixPos byte = 0x00
	msgUint8  byte = 0xcc
	msgUint16 byte = 0xcd
	msgUint32 byte = 0xce
	msgUint64 byte = 0xcf
	msgFixRaw byte = 0xa0
	msgRaw16  byte = 0xda
	msgRaw32  byte = 0xdb
)

type BlockHandle struct {
	offset uint64
	length uint64
}

type IndexRecord struct {
	offset uint64
	length uint32
	name   []byte
}

type TabletOptions struct {
	BlockSize          uint32
	BlockEncoding      BlockEncodingType
	BlockCompression   BlockCompressionType
	KeyRestartInterval uint
}

func WriteTablet(w io.Writer, kvs Iterator, opts *TabletOptions) {
	headLen := uint64(writeHeader(w, opts))
	dataBlocks := writeDataBlocks(w, kvs, headLen, opts)
	metaIndexLen := writeIndex(w, metaIndexMagic, nil)

	dataIndexLen := writeIndex(w, dataIndexMagic, dataBlocks)

	lastBlock := dataBlocks[len(dataBlocks)-1]
	dataLen := lastBlock.offset + uint64(lastBlock.length)

	metaIndexHandle := BlockHandle{dataLen, metaIndexLen}
	dataIndexHandle := BlockHandle{dataLen + metaIndexLen, dataIndexLen}

	writeFooter(w, metaIndexHandle, dataIndexHandle)
}

func writeDataBlocks(w io.Writer, kvs Iterator, pos uint64, opts *TabletOptions) []*IndexRecord {
	index := make([]*IndexRecord, 0)
	builder := NewBlockBuilder(opts)

	finishBlock := func(builder BlockBuilder) {
		firstKey, block := builder.Finish()
		if firstKey != nil {
			comp, _ := compress(opts.BlockCompression, block)

			index = append(index,
				&IndexRecord{pos, uint32(len(comp)), firstKey})

			w.Write(comp)
			pos += uint64(len(comp))
			builder.Reset()
		}
	}

	var prevKey []byte
	for kvs.Next() {
		key := kvs.Key()
		if prevKey != nil && bytes.Compare(prevKey, key) >= 0 {
			log.Printf("writing non-increasing keys: %s -> %s",
				prevKey, key)
		}

		builder.Append(kvs.Key(), kvs.Value())

		if builder.Size() > opts.BlockSize {
			finishBlock(builder)
		}
	}

	kvs.Close()

	finishBlock(builder)

	return index
}

func compress(t BlockCompressionType, input []byte) ([]byte, error) {
	var buf bytes.Buffer

	switch t {
	case Snappy:
		comp, err := snappy.Encode(nil, input)
		if len(comp) > len(input) {
			// no gains, write an uncompressed block
			buf.Write([]byte{byte(None)})
			buf.Write(input)
			return buf.Bytes(), err
		} else {
			buf.Write([]byte{byte(Snappy)})
			buf.Write(comp)
			return buf.Bytes(), err
		}
	default:
		return input, nil
	}
}

func writeKv(w io.Writer, key []byte, value []byte) uint32 {
	keyCount := writeRaw(w, key)
	valCount := writeRaw(w, value)

	return keyCount + valCount
}

func writeRaw(w io.Writer, raw []byte) uint32 {
	n1 := writeRawHeader(w, len(raw))
	n2, _ := w.Write(raw)

	return n1 + uint32(n2)
}

func writeRawHeader(w io.Writer, n int) uint32 {
	if n < 32 {
		binary.Write(w, binary.BigEndian, msgFixRaw|byte(n))
		return 1
	} else if n < 65536 {
		w.Write([]byte{msgRaw16})
		binary.Write(w, binary.BigEndian, uint16(n))
		return 3
	} else {
		w.Write([]byte{msgRaw32})
		binary.Write(w, binary.BigEndian, uint32(n))
		return 5
	}
}

func writeUint(w io.Writer, n uint) (int, error) {
	if n <= 0x7f {
		return w.Write([]byte{byte(n)})
	} else if n <= 0xff {
		return w.Write([]byte{msgUint8, byte(n)})
	} else if n <= 0xffff {
		w.Write([]byte{msgUint16})
		err := binary.Write(w, binary.BigEndian, uint16(n))
		return 3, err
	} else if n <= 0xffffffff {
		w.Write([]byte{msgUint32})
		err := binary.Write(w, binary.BigEndian, uint32(n))
		return 5, err
	}

	return 0, errors.New("uint too large")
}

func writeUint32(w io.Writer, n uint32) uint32 {
	n1, _ := w.Write([]byte{msgUint32})
	binary.Write(w, binary.BigEndian, n)

	return uint32(n1) + 4
}

func writeUint64(w io.Writer, n uint64) uint32 {
	n1, _ := w.Write([]byte{msgUint64})
	binary.Write(w, binary.BigEndian, n)

	return uint32(n1) + 8
}

func writeIndex(w io.Writer, magic uint32, recs []*IndexRecord) uint64 {
	binary.Write(w, binary.BigEndian, magic)

	var n uint64
	for _, rec := range recs {
		n += uint64(writeUint64(w, rec.offset))
		n += uint64(writeUint32(w, rec.length))
		n += uint64(writeRaw(w, rec.name))
	}

	// include 4 bytes for the magic number
	return n + 4
}

func writeBinary(w io.Writer, data interface{}) (uint32, error) {
	n := uint32(binary.Size(data))
	err := binary.Write(w, binary.BigEndian, data)
	return n, err
}

func writeHeader(w io.Writer, opts *TabletOptions) uint32 {
	h := Header{tabletMagic, opts.BlockEncoding, opts.BlockCompression, 0, 0}
	n, _ := writeBinary(w, &h)
	return n
}

func writeFooter(w io.Writer, meta BlockHandle, data BlockHandle) {
	writeUint64(w, meta.offset)
	writeUint64(w, meta.length)
	writeUint64(w, data.offset)
	writeUint64(w, data.length)
	binary.Write(w, binary.BigEndian, tabletMagic)
}

type BlockBuilder interface {
	Append(key []byte, value []byte)
	Size() uint32

	// returns the first key and the encoded key-value block
	Finish() ([]byte, []byte)
	Reset()
}

func NewBlockBuilder(opts *TabletOptions) BlockBuilder {
	switch opts.BlockEncoding {
	case Raw:
		return NewRawBlockBuilder(opts)
	default:
		return nil
	}
}

type RawBlockBuilder struct {
	opts     *TabletOptions
	buf      *bytes.Buffer
	firstKey []byte
}

func NewRawBlockBuilder(opts *TabletOptions) *RawBlockBuilder {
	// initialize to 2*BlockSize to minimize resizes
	buf := bytes.NewBuffer(make([]byte, 0, 2*opts.BlockSize))
	return &RawBlockBuilder{opts, buf, nil}
}

func (b *RawBlockBuilder) Append(key []byte, value []byte) {
	if b.buf.Len() == 0 {
		b.firstKey = key
	}

	writeKv(b.buf, key, value)
}

func (b *RawBlockBuilder) Size() uint32 {
	return uint32(b.buf.Len())
}

func (b *RawBlockBuilder) Finish() (firstKey []byte, buf []byte) {
	return b.firstKey, b.buf.Bytes()
}

func (b *RawBlockBuilder) Reset() {
	b.firstKey = nil
	b.buf.Reset()
}
