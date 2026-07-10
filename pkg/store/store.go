// Package store wraps a SQLite database (via the pure-Go modernc.org/sqlite
// driver, so cross-compiling this project -- notably to Android -- needs no
// CGO toolchain) as the on-disk key/value backend for a single raft node's
// FSM.
package store

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("store: key not found")

// Store is a durable key/value store backed by SQLite.
type Store struct {
	db *sql.DB
}

// Open opens (creating if necessary) a SQLite database rooted at dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dsn := filepath.Join(dir, "kv.db") + "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// SQLite allows only one writer at a time regardless of connection count,
	// and raft already serializes every mutation through FSM.Apply -- so a
	// single shared connection just avoids SQLITE_BUSY lock-contention
	// errors between that writer and concurrent Get reads (e.g. from an IPC
	// handler goroutine) instead of buying any real parallelism.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB NOT NULL)`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Get returns the value for key, or ErrNotFound if it does not exist.
func (s *Store) Get(key []byte) ([]byte, error) {
	var v []byte
	err := s.db.QueryRow(`SELECT value FROM kv WHERE key = ?`, key).Scan(&v)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return v, nil
}

// Set writes key/value durably.
func (s *Store) Set(key, value []byte) error {
	_, err := s.db.Exec(`INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// Delete removes key.
func (s *Store) Delete(key []byte) error {
	_, err := s.db.Exec(`DELETE FROM kv WHERE key = ?`, key)
	return err
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// DumpAll writes every key/value pair currently in the store to w, encoded
// as [4-byte big-endian key length][key][4-byte big-endian value length]
// [value], repeated. It is used by the raft FSM snapshot.
func (s *Store) DumpAll(w io.Writer) error {
	rows, err := s.db.Query(`SELECT key, value FROM kv`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var lenBuf [4]byte
	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
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
	return rows.Err()
}

// LoadAll replaces the store's contents with the key/value pairs read from
// r, in the format produced by DumpAll. It is used by the raft FSM restore.
func (s *Store) LoadAll(r io.Reader) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM kv`); err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT INTO kv(key, value) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

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
		if _, err := stmt.Exec(key, value); err != nil {
			return err
		}
	}
	return tx.Commit()
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
