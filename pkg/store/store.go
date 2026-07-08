// Package store wraps a cockroachdb/pebble database as the on-disk key/value
// backend for a single raft node's FSM.
package store

import (
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
)

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("store: key not found")

// Store is a durable key/value store backed by Pebble.
type Store struct {
	db *pebble.DB
}

// Open opens (creating if necessary) a Pebble database rooted at dir.
func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Get returns the value for key, or ErrNotFound if it does not exist.
func (s *Store) Get(key []byte) ([]byte, error) {
	v, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, closer.Close()
}

// Set writes key/value durably.
func (s *Store) Set(key, value []byte) error {
	return s.db.Set(key, value, pebble.Sync)
}

// Delete removes key.
func (s *Store) Delete(key []byte) error {
	return s.db.Delete(key, pebble.Sync)
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// DumpAll writes every key/value pair currently in the store to w, encoded
// as [4-byte big-endian key length][key][4-byte big-endian value length]
// [value], repeated. It is used by the raft FSM snapshot.
func (s *Store) DumpAll(w io.Writer) error {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	var lenBuf [4]byte
	for valid := iter.First(); valid; valid = iter.Next() {
		k := iter.Key()
		v, err := iter.ValueAndErr()
		if err != nil {
			return err
		}
		putUint32(lenBuf[:], uint32(len(k)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.Write(k); err != nil {
			return err
		}
		putUint32(lenBuf[:], uint32(len(v)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.Write(v); err != nil {
			return err
		}
	}
	return iter.Error()
}

// LoadAll replaces the store's contents with the key/value pairs read from
// r, in the format produced by DumpAll. It is used by the raft FSM restore.
func (s *Store) LoadAll(r io.Reader) error {
	if err := s.deleteAll(); err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		key := make([]byte, getUint32(lenBuf[:]))
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return err
		}
		value := make([]byte, getUint32(lenBuf[:]))
		if _, err := io.ReadFull(r, value); err != nil {
			return err
		}
		if err := batch.Set(key, value, nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) deleteAll() error {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := s.db.NewBatch()
	defer batch.Close()
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func putUint32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func getUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
