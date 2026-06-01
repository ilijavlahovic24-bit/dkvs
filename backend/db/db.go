package db

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var defaultBucket = []byte("default")

type DB struct {
	db *bolt.DB
}

func NewDatabase(dbpath string) (db *DB, closeFunc func() error, err error) {
	boltDb, err := bolt.Open(dbpath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &DB{db: boltDb}
	closeFunc = boltDb.Close

	if err := db.createBucketIfNotExists(); err != nil {
		closeFunc()
		return nil, nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	return db, closeFunc, nil
}
func (d *DB) createBucketIfNotExists() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucket)
		return err
	})
}
func (d *DB) Set(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.Put([]byte(key), value)

	})
}
func (d *DB) Get(key string) ([]byte, error) {
	var result []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		result = b.Get([]byte(key))
		return nil
	})
	if err == nil {
		return result, nil
	}
	return nil, err
}
