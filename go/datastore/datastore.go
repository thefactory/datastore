package datastore

import (
	"bytes"
	"errors"
	"sync"
)

type Datastore struct {
	mutex   sync.Mutex
	tablets []*Tablet
}

func (ds *Datastore) PushTablet(filename string) error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	tab, err := OpenTabletFile(filename)
	if err != nil {
		return err
	}

	ds.tablets = append(ds.tablets, tab)

	return nil
}

func (ds *Datastore) PopTablet() {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	if len(ds.tablets) > 0 {
		ds.tablets = ds.tablets[:len(ds.tablets)-1]
	}
}

func (ds *Datastore) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("nil key Get()")
	}

	iter := ds.Find(key)

	if iter.Next() && bytes.Compare(key, iter.Key()) == 0 {
		return iter.Value(), nil
	}

	return nil, errors.New("key not found")
}

func (ds *Datastore) Find(key []byte) Iterator {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	return Merge(len(ds.tablets), func(n int) Iterator {
		return ds.tablets[n].Find(key)
	})
}

func (ds *Datastore) Close() error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	ds.tablets = nil

	return nil
}
