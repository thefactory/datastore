package transaction

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
)

const (
	blockSize = 32768
	headerLen = 7
)

const (
	fullRecordType   = 1
	firstRecordType  = 2
	middleRecordType = 3
	lastRecordType   = 4
)

type Writer struct {
	f    *os.File
	left int
	buf  *bytes.Buffer
}

func NewFileWriter(file string) (*Writer, error) {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(make([]byte, 0, blockSize))

	return &Writer{f, blockSize, buf}, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (w *Writer) Write(data []byte) error {
	var isFirst bool = true

	// the isFirst check here ensures at least one record is written
	for isFirst || len(data) > 0 {
		if w.left < headerLen {
			// a full block won't fit: finish this one
			w.f.Write(make([]byte, w.left))
			w.left = blockSize
		}

		// figure out how much data can be written
		size := min(len(data), w.left-headerLen)

		recordType := getRecordType(isFirst, size, len(data))
		record := data[:size]

		buf := w.buf
		binary.Write(buf, binary.BigEndian, crc32.ChecksumIEEE(record))
		binary.Write(buf, binary.BigEndian, byte(recordType))
		binary.Write(buf, binary.BigEndian, uint16(size))
		buf.Write(record)

		w.f.Write(buf.Bytes())

		data = data[size:]
		w.left -= buf.Len()
		isFirst = false
		buf.Reset()
	}

	return w.f.Sync()
}

func getRecordType(isFirst bool, size, remaining int) int {
	if isFirst {
		if size == remaining {
			return fullRecordType
		} else {
			return firstRecordType
		}
	} else {
		if size == remaining {
			return lastRecordType
		} else {
			return middleRecordType
		}
	}
}
