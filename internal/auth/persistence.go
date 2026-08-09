package auth

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	bolt "go.etcd.io/bbolt"
)

var (
	authBucket = []byte("authorization")
	authKey    = []byte("state-v1")
)

type boltPersistence struct {
	db *bolt.DB
}

// OpenBoltPersistence opens the durable Phase 1 Device Authorization state.
// The database and parent directory must be private to the service account.
func OpenBoltPersistence(path string) (Persistence, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("authorization state path must be absolute and clean")
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, err
	}
	if err := validatePrivateDirectory(parent); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
			return nil, errors.New("authorization state must be an owner-private regular file")
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Uid != uint32(os.Geteuid()) {
			return nil, errors.New("authorization state must be owned by the service user")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{NoGrowSync: false})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		db.Close()
		return nil, err
	}
	return &boltPersistence{db: db}, nil
}

func validatePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return errors.New("authorization state directory must be owner-private")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if ok && stat.Uid != uint32(os.Geteuid()) {
		return errors.New("authorization state directory must be owned by the service user")
	}
	return nil
}

func (p *boltPersistence) Load() (snapshot, error) {
	var state snapshot
	err := p.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(authBucket)
		if bucket == nil {
			return nil
		}
		value := bucket.Get(authKey)
		if len(value) == 0 {
			return nil
		}
		return json.Unmarshal(append([]byte(nil), value...), &state)
	})
	return state, err
}

func (p *boltPersistence) Save(state snapshot) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return p.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(authBucket)
		if err != nil {
			return err
		}
		return bucket.Put(authKey, encoded)
	})
}

func (p *boltPersistence) Close() error { return p.db.Close() }
