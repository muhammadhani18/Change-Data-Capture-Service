package store

import (
    "encoding/binary"
    "fmt"
    "time"

    bolt "go.etcd.io/bbolt"
    "github.com/jackc/pglogrepl"
)

const (
    bucketName = "cdc-checkpoints"
    keyName    = "lastLSN"
)

type Store struct {
    db *bolt.DB
}

// OpenStore opens (or creates) the BoltDB file and bucket.
func OpenStore(path string) (*Store, error) {
    db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
    if err != nil {
        return nil, fmt.Errorf("open bolt db: %w", err)
    }
    if err := db.Update(func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists([]byte(bucketName))
        return err
    }); err != nil {
        return nil, fmt.Errorf("create bucket: %w", err)
    }
    return &Store{db: db}, nil
}

// GetLSN returns the last saved LSN, or zero if none.
func (s *Store) GetLSN() (pglogrepl.LSN, error) {
    var lsn pglogrepl.LSN
    err := s.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(bucketName))
        v := b.Get([]byte(keyName))
        if v == nil {
            return nil
        }
        // uint64 LSN is stored big-endian
        val := binary.BigEndian.Uint64(v)
        lsn = pglogrepl.LSN(val)
        return nil
    })
    return lsn, err
}

// SaveLSN persists the given LSN into BoltDB.
func (s *Store) SaveLSN(lsn pglogrepl.LSN) error {
    return s.db.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(bucketName))
        buf := make([]byte, 8)
        binary.BigEndian.PutUint64(buf, uint64(lsn))
        return b.Put([]byte(keyName), buf)
    })
}

// Close the DB when shutting down
func (s *Store) Close() error {
    return s.db.Close()
}
