package db

import (
	"dkvs/lsm"
	"errors"
	"fmt"
	"path/filepath"
)

var defaultBucket = []byte("default")
var replicaBucket = []byte("replication")

type DB struct {
	data     *lsm.LSM
	replica  *lsm.LSM
	readOnly bool
}

func NewDatabase(dbPath string, readOnly bool) (db *DB, closeFunc func() error, err error) {
	data, err := lsm.NewLSM(dbPath)
	if err != nil {
		return nil, nil, fmt.Errorf("opening data LSM: %w", err)
	}

	replicaPath := filepath.Join(dbPath, "replication")
	replica, err := lsm.NewLSM(replicaPath)
	if err != nil {
		data.Close()
		return nil, nil, fmt.Errorf("opening replica LSM: %w", err)
	}

	db = &DB{
		data:     data,
		replica:  replica,
		readOnly: readOnly,
	}

	closeFunc = func() error {
		var firstErr error
		if err := replica.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := data.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}

	return db, closeFunc, nil
}

/*
func (d *DB) createBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(replicaBucket); err != nil {
			return err
		}
		return nil
	})
}
*/
// SetKey sets the key to the requested value into the default database or returns an error.
func (d *DB) SetKey(key string, value []byte) error {
	if d.readOnly {
		return errors.New("read-only mode")
	}
	if err := d.data.Set(key, value); err != nil {
		return err
	}
	return d.replica.Set(key, value)
}

// SetKeyOnReplica sets the key to the requested value into the default database and does not write
// to the replication queue.
// This method is intended to be used only on replicas.
func (d *DB) SetKeyOnReplica(key string, value []byte) error {
	return d.data.Set(key, value)
}

func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}

// GetNextKeyForReplication returns the key and value for the keys that have
// changed and have not yet been applied to replicas.
// If there are no new keys, nil key and value will be returned.
func (d *DB) GetNextKeyForReplication() (key, value []byte, err error) {
	var k string
	var v []byte
	iterErr := d.replica.Iterate(func(key string, value []byte) bool {
		k = key
		v = value
		return false // early exit
	})
	if iterErr != nil {
		return nil, nil, iterErr
	}
	if k == "" {
		return nil, nil, nil
	}
	return []byte(k), v, nil
}

// DeleteReplicationKey deletes the key from the replication queue
// if the value matches the contents or if the key is already absent.
func (d *DB) DeleteReplicationKey(key, value []byte) (err error) {
	existing, err := d.replica.Get(string(key))
	if errors.Is(err, lsm.ErrNotFound) {
		return errors.New("key does not exist")
	}
	if err != nil {
		return err
	}
	if string(existing) != string(value) {
		return errors.New("value does not match")
	}
	return d.replica.Delete(string(key))
}

// GetKey get the value of the requested from a default database.
func (d *DB) GetKey(key string) ([]byte, error) {
	v, err := d.data.Get(key)
	if errors.Is(err, lsm.ErrNotFound) {
		return nil, nil
	}
	return v, err
}

// DeleteExtraKeys deletes the keys that do not belong to this shard.
func (d *DB) DeleteExtraKeys(isExtra func(string) bool) error {
	if d.readOnly {
		return errors.New("read-only mode")
	}

	// Prvo skupi ključeve (ne možemo brisati tokom iteracije — RLock vs Lock).
	var keys []string
	if err := d.data.Iterate(func(key string, value []byte) bool {
		if isExtra(key) {
			keys = append(keys, key)
		}
		return true
	}); err != nil {
		return err
	}

	for _, k := range keys {
		if err := d.data.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) KeyCount() int {
	count := 0
	_ = d.data.Iterate(func(key string, value []byte) bool {
		count++
		return true
	})
	return count
}
