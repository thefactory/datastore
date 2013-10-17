package transaction

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
)

var ChecksumErr error

func init() {
	ChecksumErr = errors.New("Bad record checksum")
}

type Reader struct {
	f      *os.File
	header []byte
	buf    *bytes.Buffer
}

func NewFileReader(file string) (*Reader, error) {
	f, err := os.OpenFile(file, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	header := make([]byte, headerLen)

	// buf will be resized for transactions larger than blockSize
	buf := bytes.NewBuffer(make([]byte, 0, blockSize))

	return &Reader{f, header, buf}, nil
}

func (r *Reader) Close() error {
	r.header = nil
	r.buf = nil
	return r.f.Close()
}

func (r *Reader) Next() bool {
	r.buf.Reset()

	recordType, err := r.readRecord()
	if err != nil {
		return false
	}

	for recordType != fullRecordType && recordType != lastRecordType {
		recordType, _ = r.readRecord()
	}

	return true
}

func (r *Reader) readRecord() (int, error) {
	_, err := r.f.Read(r.header)
	if err != nil {
		return 0, err
	}

	header := r.header
	checksum := binary.BigEndian.Uint32(header[:4])
	recordType := int(header[4])
	length := binary.BigEndian.Uint16(header[5:])

	// it might be nice to use a longer lived buffer here
	tmp := make([]byte, length)
	_, err = r.f.Read(tmp)
	if err != nil {
		return 0, err
	}

	if checksum != crc32.ChecksumIEEE(tmp) {
		return 0, ChecksumErr
	}

	r.buf.Write(tmp)

	return recordType, nil
}

func (r *Reader) Transaction() []byte {
	return r.buf.Bytes()
}
