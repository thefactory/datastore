The Datastore
=============

The Factory's Datastore is a embedded, sorted key-value database
designed for high performance on mobile devices. It enables rapid
synchronization between a server and a mobile client: rather than
delivering data, the server sends the database. Changed values can
later be applied as a delta to the original database with minimal
processing.

Much of the design of this datastore has been inspired by LevelDB: it
operates using immutable tables of key-value pairs. The format of
these files is similar to LevelDB's, with internal structures encoded
using msgpack for better integration with our stack.

Our key-value blocks are encoded with a header that allows them to be
read before the entire tablet file is available, so an application can
show data during its sync process.
