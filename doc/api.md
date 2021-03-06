Factory Datastore API
=====================

This describes the generic database API provided by a datastore
client. These interfaces are described in Go but should be easily
translated to other languages. There are three levels of support:
read-only, read-write, and read-write with secondary indexes.


Read-only Datastore API
-----------------------

A read-only API provides `Get(key)` and Iterator based APIs for
reading collections of tablets.

For read-only support, the following components are necessary:

* A tablet reader

  This is a parser for the on-disk tablet format described in
  `tablet.md`. It must parse the tablet footer, read the metadata and
  data block indexes, and provide iterator support (described below)
  for the tablet's key-value pairs.

  Necessary subcomponents are a key-value block reader and the ability
  to binary search the block index and block restarts indexes.

  A Snappy decompressor will be required, though compression may be
  turned off per-application if one is unavailable.

* An iterator merger

  In order to combine several tablets into a single view, we must
  merge together their iterators. Since these provide key-value pairs
  in sorted order, each can be treated as a stream. At each step, we
  yield the minimum key-value from all iterators. When the same key is
  available on several iterators, its value will be yielded from the
  tablet most recently added to the datastore.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// An Iterator for a series of key-value pairs. It's acceptable to //
// provide key/value data that is only valid until Next() is called
// again.
type Iterator interface {
     // Advances the iterator to the next value. This must be called
     // once before Key() and Value() will return valid results.
     Next() bool

     // Return the key and value at current iteration point.
     Key() []byte
     Value() []byte

     // Close the iterator and release any resources.
     Close() error
}

// Access to an ordered key-value datastore. This encapsulates a series
// of tablet files, merging them together for iteration. Tablet files are
// searched in the reverse order they were added to the datastore, with
// the most recently added tablet searched first.
//
// When iterating over a datastore (as in the results of `Find()`),
// the tablet files are merged together with the newest value for any
// key winning.
type Datastore interface {
     // Add the specified file as the newest tablet in the datastore.
     PushTablet(filename string) error
     PopTablet() (string, error)

     // Get the most recent value associated with `key`. Depending on
     // the language where this is implemented, it may throw an error
     // or return a null value for missing keys
     Get(key []byte) (value []byte, err error)

     // Get an iterator over the key-value pairs in datastore. This
     // yields items in order, sorted by key. The first item yielded
     // must be the first key greater than or equal to the requested
     // key.
     Find(key []byte) Iterator

     // Release any resources.
     Close() error
}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Thread safety considerations: the above should be thread-safe with
multiple readers iterating over the same database. A running iterator
need not be updated when new tablet files are pushed or popped.


Read-write Datastore API
------------------------

For read-write support, a few more components are required:

* An in-memory tablet

  This gathers newly written items in memory before they're written to
  an immutable tablet. This must provide in-order iterative access as
  well as key `Set()` and `Delete()`, so the recommended
  implementation includes a sorted data structure: consider a built-in
  sorted dictionary, if one is available, or implement a skiplist.

* A transaction logger

  This logs new key-value pairs to disk for durability as they're
  inserted into the in-memory tablet. On application restart, the
  active in-memory tablet will be rebuilt from the log.

* A tablet writer

  This writes key-value pairs from the in-memory tablet into a new
  immutable tablet file.

  The tablet writer can operate on an Iterator of key-value pairs,
  building blocks and flushing them to its writer as they get larger
  than opts.BlockSize.

  See the Go implementation of WriteTablet() for program flow.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
type WritableDatastore interface {
     Put(key []byte, value []byte)
     Delete(key []byte)
}

type BlockWriter interface {
     Append(key []byte, value []byte)

     // an estimate of the block size written so far
     Size() uint32

     Finish() (firstKey []byte, buf []byte)

     // prepare this block writer for writing another block
     Reset()
}

type TabletOptions struct {
     BlockSize          uint32
     BlockCompression   bool
     KeyRestartInterval uint
}

type TabletWriter interface {
     WriteTablet(writer io.Writer, kvs Iterator, opts TabletOptions)
}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
