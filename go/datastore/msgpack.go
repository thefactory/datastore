package datastore

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
)

// Lightweight msgpack encode/decode functions

const (
	msgFixPos byte = 0x00
	msgUint8  byte = 0xcc
	msgUint16 byte = 0xcd
	msgUint32 byte = 0xce
	msgUint64 byte = 0xcf
	msgFixRaw byte = 0xa0
	msgRaw16  byte = 0xda
	msgRaw32  byte = 0xdb
)

func writeUint32(w io.Writer, n uint32) uint32 {
	n1, _ := w.Write([]byte{msgUint32})
	binary.Write(w, binary.BigEndian, n)

	return uint32(n1) + 4
}

func readUint32(r io.Reader) uint32 {
	var junk byte
	var ret uint32

	binary.Read(r, binary.BigEndian, &junk)
	binary.Read(r, binary.BigEndian, &ret)
	return ret
}

func writeUint64(w io.Writer, n uint64) uint32 {
	n1, _ := w.Write([]byte{msgUint64})
	binary.Write(w, binary.BigEndian, n)

	return uint32(n1) + 8
}

func readUint64(r io.Reader) uint64 {
	var junk byte
	var ret uint64

	binary.Read(r, binary.BigEndian, &junk)
	binary.Read(r, binary.BigEndian, &ret)
	return ret
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

func readUint(r io.Reader) uint {
	var flag byte
	binary.Read(r, binary.BigEndian, &flag)

	if flag <= 0x7f {
		return uint(flag)
	} else if flag == msgUint8 {
		var value uint8
		binary.Read(r, binary.BigEndian, &value)
		return uint(value)
	} else if flag == msgUint16 {
		var value uint16
		binary.Read(r, binary.BigEndian, &value)
		return uint(value)
	} else if flag == msgUint32 {
		var value uint32
		binary.Read(r, binary.BigEndian, &value)
		return uint(value)
	}

	return 0
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

func readRaw(r io.Reader) []byte {
	var flag byte
	binary.Read(r, binary.BigEndian, &flag)

	var length uint32
	if flag&msgFixRaw == msgFixRaw {
		length = uint32(flag &^ msgFixRaw)
	} else if flag == msgRaw16 {
		var tmp uint16
		binary.Read(r, binary.BigEndian, &tmp)
		length = uint32(tmp)
	} else if flag == msgRaw32 {
		binary.Read(r, binary.BigEndian, &length)
	} else {
		log.Printf("ERROR: bad flag in readRaw: 0x%x", flag)
	}

	buf := make([]byte, length)
	r.Read(buf)
	return buf
}

func peekRaw(data []byte) []byte {
	var flag byte = data[0]
	var ret []byte

	if flag&msgFixRaw == msgFixRaw {
		length := uint32(flag & (^msgFixRaw))
		ret = data[1 : length+1]
	} else if flag == msgRaw16 {
		length := uint32(binary.BigEndian.Uint16(data[1:3]))
		ret = data[3 : length+3]
	} else if flag == msgRaw32 {
		length := uint32(binary.BigEndian.Uint32(data[1:5]))
		ret = data[5 : length+5]
	}

	return ret
}
