package main

// Open ngrams1.txt and write its data to a few tablet files as
// described in README.

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/thefactory.com/datastore/go/datastore"
)

func main() {
	file, err := os.Open("ngrams1.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	writeTablet("ngrams1-1block-uncompressed.tab", lines,
		&datastore.TabletOptions{
			BlockSize:          1024 * 1024,
			BlockCompression:   datastore.None,
			KeyRestartInterval: 500,
		})

	writeTablet("ngrams1-1block-compressed.tab", lines,
		&datastore.TabletOptions{
			BlockSize:          1024 * 1024,
			BlockCompression:   datastore.Snappy,
			KeyRestartInterval: 5,
		})

	writeTablet("ngrams1-Nblock-compressed.tab", lines,
		&datastore.TabletOptions{
			BlockSize:          1,
			BlockCompression:   datastore.Snappy,
			KeyRestartInterval: 5,
		})
}

func writeTablet(filename string, ngrams []string, opts *datastore.TabletOptions) error {
	fd, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	datastore.WriteTablet(fd, NewNgramIterator(ngrams), opts)
	return nil
}

type NgramIterator struct {
	ngrams []string
	key    []byte
	val    []byte
}

func NewNgramIterator(ngrams []string) *NgramIterator {
	return &NgramIterator{ngrams: ngrams}
}

func (iter *NgramIterator) Next() bool {
	if len(iter.ngrams) == 0 {
		iter.key = nil
		iter.val = nil
		return false
	}

	kv := strings.Split(iter.ngrams[0], " ")
	iter.key = []byte(kv[0])
	iter.val = []byte(kv[1])

	iter.ngrams = iter.ngrams[1:]

	return true
}

func (iter *NgramIterator) Key() []byte {
	return iter.key
}

func (iter *NgramIterator) Value() []byte {
	return iter.val
}

func (iter *NgramIterator) Close() error {
	return nil
}
