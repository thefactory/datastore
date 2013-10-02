Factory Datastore API
=====================

This describes the generic database API provided by a datastore
client. These interfaces are described in Go but should be easily
translated to other languages. There are three levels of support:
read-only, read-write, and read-write with secondary indexes.


Read-only Datastore API
-----------------------

Datastore provides `Get(key)` and iterator based APIs for reading
collections of tablets.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// An Iterator for a series of key-value pairs.
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
