package datastore

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"encoding/binary"
	"io"
	"log"
)

const (
	tabletMagic    uint32 = 0x0b501e7e
	metaIndexMagic uint32 = 0x0ea7da7a
	dataIndexMagic uint32 = 0xda7aba5e
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
	BlockCompression   BlockCompressionType
	KeyRestartInterval uint
}

func WriteTablet(w io.Writer, kvs Iterator, opts *TabletOptions) {
	headLen := uint64(writeHeader(w, opts))
	dataBlocks := writeDataBlocks(w, kvs, headLen, opts)
	metaIndexLen := writeIndex(w, metaIndexMagic, nil)

	dataIndexLen := writeIndex(w, dataIndexMagic, dataBlocks)

	lastBlock := dataBlocks[len(dataBlocks)-1]
	metaPos := lastBlock.offset + uint64(lastBlock.length)

	metaIndexHandle := BlockHandle{metaPos, metaIndexLen}
	dataIndexHandle := BlockHandle{metaPos + metaIndexLen, dataIndexLen}

	writeFooter(w, metaIndexHandle, dataIndexHandle)
}

func writeDataBlocks(w io.Writer, kvs Iterator, pos uint64, opts *TabletOptions) []*IndexRecord {
	index := make([]*IndexRecord, 0)
	bw := NewBlockWriter(opts)

	flushBlock := func() {
		rec := writeBlock(w, pos, bw, opts)
		bw.Reset()

		if rec == nil {
			return
		}

		index = append(index, rec)
		pos += uint64(rec.length)
	}

	var prevKey []byte
	for kvs.Next() {
		key := kvs.Key()
		if prevKey != nil && bytes.Compare(prevKey, key) >= 0 {
			log.Printf("writing non-increasing keys: %s -> %s",
				prevKey, key)
		}

		bw.Append(kvs.Key(), kvs.Value())

		if bw.Size() > opts.BlockSize {
			flushBlock()
		}
	}

	flushBlock()

	kvs.Close()
	return index
}

func writeBlock(w io.Writer, pos uint64, bw *BlockWriter, opts *TabletOptions) *IndexRecord {
	firstKey, block := bw.Finish()
	if firstKey == nil {
		// empty block
		return nil
	}

	comp, blockType, _ := compress(opts, block)

	var checksum uint32 = 0
	var length uint32 = uint32(len(comp))

	a := writeUint(w, uint(checksum))
	b := writeUint(w, uint(blockType))
	c := writeUint(w, uint(length))

	length += uint32(a + b + c)

	w.Write(comp)

	return &IndexRecord{pos, length, firstKey}
}

func compress(opts *TabletOptions, input []byte) ([]byte, BlockCompressionType, error) {
	var buf bytes.Buffer

	switch opts.BlockCompression {
	case Snappy:
		comp, err := snappy.Encode(nil, input)
		if len(comp) > len(input) {
			// no gains, write an uncompressed block
			buf.Write(input)
			return buf.Bytes(), None, err
		} else {
			buf.Write(comp)
			return buf.Bytes(), Snappy, err
		}
	default:
		return input, None, nil
	}
}

func writeKv(w io.Writer, key []byte, value []byte) uint32 {
	keyCount := writeRaw(w, key)
	valCount := writeRaw(w, value)

	return keyCount + valCount
}

func writeIndex(w io.Writer, magic uint32, recs []*IndexRecord) uint64 {
	binary.Write(w, binary.BigEndian, magic)

	var n uint64
	for _, rec := range recs {
		n += uint64(writeUint(w, uint(rec.offset)))
		n += uint64(writeUint(w, uint(rec.length)))
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
	h := Header{tabletMagic, BlockEncodingType(PrefixCompressed), 0, 0, 0}
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
