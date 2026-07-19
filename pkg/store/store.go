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

// Set writes key/value durably. A nil value is normalized to an empty
// (but non-nil) one first: database/sql binds a nil []byte parameter as
// SQL NULL, which the kv table's value column (NOT NULL) rejects --
// without this, a value that happens to be nil (e.g. one just read back
// via Get, which can itself return nil for a previously-stored empty
// value) could never be Set again elsewhere, silently breaking any
// read-then-write caller (see kvfsm's OpConfirm, the first such caller).
func (s *Store) Set(key, value []byte) error {
	if value == nil {
		value = []byte{}
	}
	_, err := s.db.Exec(`INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// Delete removes key.
func (s *Store) Delete(key []byte) error {
	_, err := s.db.Exec(`DELETE FROM kv WHERE key = ?`, key)
	return err
}

// CountPrefix returns how many stored keys begin with prefix -- a range
// scan over the BLOB-keyed kv table (SQLite compares BLOBs byte-wise, so
// key >= prefix AND key < prefixUpperBound(prefix) selects exactly that
// range). Used by pkg/kvfsm to enforce a per-SystemKey-list entry cap.
func (s *Store) CountPrefix(prefix []byte) (int, error) {
	upper := prefixUpperBound(prefix)
	var row *sql.Row
	if upper == nil {
		row = s.db.QueryRow(`SELECT COUNT(*) FROM kv WHERE key >= ?`, prefix)
	} else {
		row = s.db.QueryRow(`SELECT COUNT(*) FROM kv WHERE key >= ? AND key < ?`, prefix, upper)
	}
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// KV is one key/value pair, as returned by ScanRange/ScanPrefix.
type KV struct {
	Key   []byte
	Value []byte
}

// ScanRange returns every stored pair with start <= key <= end, ascending
// by key (SQLite compares BLOBs byte-wise, so this is a plain lexical
// range over the raw key bytes -- see pkg/logrecord for a key scheme
// built around that property). limit caps how many pairs are returned;
// limit <= 0 means unlimited.
func (s *Store) ScanRange(start, end []byte, limit int) ([]KV, error) {
	query := `SELECT key, value FROM kv WHERE key >= ? AND key <= ? ORDER BY key`
	args := []any{start, end}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []KV
	for rows.Next() {
		var kv KV
		if err := rows.Scan(&kv.Key, &kv.Value); err != nil {
			return nil, err
		}
		out = append(out, kv)
	}
	return out, rows.Err()
}

// ScanPrefix returns every stored pair whose key begins with prefix,
// ascending by key -- the same prefixUpperBound range trick CountPrefix
// already uses (key >= prefix AND key < prefixUpperBound(prefix)), except
// selecting the rows themselves instead of just counting them. Unlike
// ScanRange, the upper bound here is exclusive, since prefixUpperBound
// returns the smallest key past every key starting with prefix, not a
// member of that set itself. limit caps how many pairs are returned;
// limit <= 0 means unlimited.
func (s *Store) ScanPrefix(prefix []byte, limit int) ([]KV, error) {
	upper := prefixUpperBound(prefix)
	query := `SELECT key, value FROM kv WHERE key >= ?`
	args := []any{prefix}
	if upper != nil {
		query += ` AND key < ?`
		args = append(args, upper)
	}
	query += ` ORDER BY key`
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []KV
	for rows.Next() {
		var kv KV
		if err := rows.Scan(&kv.Key, &kv.Value); err != nil {
			return nil, err
		}
		out = append(out, kv)
	}
	return out, rows.Err()
}

// prefixUpperBound returns the smallest byte sequence greater than every
// sequence starting with prefix -- prefix with its last non-0xFF byte
// incremented, carrying into preceding bytes as needed (the standard
// BLOB-prefix-range trick). Returns nil if prefix is empty or entirely
// 0xFF bytes (no finite upper bound exists), in which case the caller
// scans with no upper bound at all.
func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] < 0xFF {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
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
