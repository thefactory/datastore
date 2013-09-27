package datastore

import (
	"encoding/binary"
	"io"
	"log"
)

// Lightweight msgpack encode/decode functions

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
