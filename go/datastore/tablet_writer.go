package datastore

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
)

const (
	footerMagic    uint32 = 0x0b501e7e
	metaIndexMagic uint32 = 0x0ea7da7a
	dataIndexMagic uint32 = 0xda7aba5e

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

func WriteTablet(w io.Writer, kvs []KV) {
	firstKey := kvs[0].Key

	dataLen, _ := writeKvs(w, kvs)
	metaIndexLen := writeIndex(w, metaIndexMagic, nil)

	// For the moment we only support one data index record, so
	// its length must be cast to uint32 to fit.
	rec := IndexRecord{0, uint32(dataLen), firstKey}
	dataIndexLen := writeIndex(w, dataIndexMagic, []IndexRecord{rec})

	metaIndexHandle := BlockHandle{dataLen, metaIndexLen}
	dataIndexHandle := BlockHandle{dataLen + metaIndexLen, dataIndexLen}

	writeFooter(w, metaIndexHandle, dataIndexHandle)
}

func writeKvs(w io.Writer, kvs []KV) (uint64, error) {
	var count uint64

	for i, kv := range kvs {
		if i > 0 && bytes.Compare(kvs[i-1].Key, kv.Key) >= 0 {
			log.Printf("writing non-increasing keys: %s -> %s",
				kvs[i-1].Key, kv.Key)
		}

		keyCount := writeRaw(w, kv.Key)
		valCount := writeRaw(w, kv.Value)

		count += uint64(keyCount) + uint64(valCount)
	}

	return count, nil
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

func writeIndex(w io.Writer, magic uint32, recs []IndexRecord) uint64 {
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

func writeFooter(w io.Writer, meta BlockHandle, data BlockHandle) {
	writeUint64(w, meta.offset)
	writeUint64(w, meta.length)
	writeUint64(w, data.offset)
	writeUint64(w, data.length)
	binary.Write(w, binary.BigEndian, footerMagic)
}
