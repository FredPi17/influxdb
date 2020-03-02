package bolt

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/kv"
	"go.uber.org/zap"
)

// check that *KVStore implement kv.Store interface.
var _ kv.Store = (*KVStore)(nil)

// KVStore is a kv.Store backed by boltdb.
type KVStore struct {
	path string
	db   *bolt.DB
	log  *zap.Logger
}

// NewKVStore returns an instance of KVStore with the file at
// the provided path.
func NewKVStore(log *zap.Logger, path string) *KVStore {
	return &KVStore{
		path: path,
		log:  log,
	}
}

// AutoMigrate returns true as the bolt KVStore is safe to migrate on initialize.
func (s *KVStore) AutoMigrate() bool {
	return true
}

// Open creates boltDB file it doesn't exists and opens it otherwise.
func (s *KVStore) Open(ctx context.Context) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Ensure the required directory structure exists.
	if err := os.MkdirAll(filepath.Dir(s.path), 0700); err != nil {
		return fmt.Errorf("unable to create directory %s: %v", s.path, err)
	}

	if _, err := os.Stat(s.path); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Open database file.
	db, err := bolt.Open(s.path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return fmt.Errorf("unable to open boltdb file %v", err)
	}
	s.db = db

	s.log.Info("Resources opened", zap.String("path", s.path))
	return nil
}

// Close the connection to the bolt database
func (s *KVStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Flush removes all bolt keys within each bucket.
func (s *KVStore) Flush(ctx context.Context) {
	_ = s.db.Update(
		func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				s.cleanBucket(tx, b)
				return nil
			})
		},
	)
}

func (s *KVStore) cleanBucket(tx *bolt.Tx, b *bolt.Bucket) {
	// nested bucket recursion base case:
	if b == nil {
		return
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		_ = v
		if err := c.Delete(); err != nil {
			// clean out nexted buckets
			s.cleanBucket(tx, b.Bucket(k))
		}
	}
}

// WithDB sets the boltdb on the store.
func (s *KVStore) WithDB(db *bolt.DB) {
	s.db = db
}

// View opens up a view transaction against the store.
func (s *KVStore) View(ctx context.Context, fn func(tx kv.Tx) error) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.db.View(func(tx *bolt.Tx) error {
		return fn(&Tx{
			tx:  tx,
			ctx: ctx,
		})
	})
}

// Update opens up an update transaction against the store.
func (s *KVStore) Update(ctx context.Context, fn func(tx kv.Tx) error) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.db.Update(func(tx *bolt.Tx) error {
		return fn(&Tx{
			tx:  tx,
			ctx: ctx,
		})
	})
}

// Backup copies all K:Vs to a writer, in BoltDB format.
func (s *KVStore) Backup(ctx context.Context, w io.Writer) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}

// Tx is a light wrapper around a boltdb transaction. It implements kv.Tx.
type Tx struct {
	tx  *bolt.Tx
	ctx context.Context
}

// Context returns the context for the transaction.
func (tx *Tx) Context() context.Context {
	return tx.ctx
}

// WithContext sets the context for the transaction.
func (tx *Tx) WithContext(ctx context.Context) {
	tx.ctx = ctx
}

// createBucketIfNotExists creates a bucket with the provided byte slice.
func (tx *Tx) createBucketIfNotExists(b []byte) (*Bucket, error) {
	bkt, err := tx.tx.CreateBucketIfNotExists(b)
	if err != nil {
		return nil, err
	}
	return &Bucket{
		bucket: bkt,
	}, nil
}

// Bucket retrieves the bucket named b.
func (tx *Tx) Bucket(b []byte) (kv.Bucket, error) {
	bkt := tx.tx.Bucket(b)
	if bkt == nil {
		return tx.createBucketIfNotExists(b)
	}
	return &Bucket{
		bucket: bkt,
	}, nil
}

// Bucket implements kv.Bucket.
type Bucket struct {
	bucket *bolt.Bucket
}

// Get retrieves the value at the provided key.
func (b *Bucket) Get(key []byte) ([]byte, error) {
	val := b.bucket.Get(key)
	if len(val) == 0 {
		return nil, kv.ErrKeyNotFound
	}

	return val, nil
}

// Put sets the value at the provided key.
func (b *Bucket) Put(key []byte, value []byte) error {
	err := b.bucket.Put(key, value)
	if err == bolt.ErrTxNotWritable {
		return kv.ErrTxNotWritable
	}
	return err
}

// Delete removes the provided key.
func (b *Bucket) Delete(key []byte) error {
	err := b.bucket.Delete(key)
	if err == bolt.ErrTxNotWritable {
		return kv.ErrTxNotWritable
	}
	return err
}

// ForwardCursor retrieves a cursor for iterating through the entries
// in the key value store in a given direction (ascending / descending).
func (b *Bucket) ForwardCursor(seek []byte, opts ...kv.CursorOption) (kv.ForwardCursor, error) {
	var (
		cursor     = b.bucket.Cursor()
		config     = kv.NewCursorConfig(opts...)
		key, value []byte
	)

	if len(seek) == 0 && config.Direction == kv.CursorDescending {
		seek, _ = cursor.Last()
	}

	key, value = cursor.Seek(seek)

	if config.Prefix != nil && !bytes.HasPrefix(seek, config.Prefix) {
		return nil, fmt.Errorf("seek bytes %q not prefixed with %q: %w", string(seek), string(config.Prefix), kv.ErrSeekMissingPrefix)
	}

	c := &Cursor{
		cursor: cursor,
		config: config,
	}

	// only remember first seeked item if not skipped
	if !config.SkipFirst {
		c.key = key
		c.value = value
	}

	return c, nil
}

// Cursor retrieves a cursor for iterating through the entries
// in the key value store.
func (b *Bucket) Cursor(opts ...kv.CursorHint) (kv.Cursor, error) {
	return &Cursor{
		cursor: b.bucket.Cursor(),
	}, nil
}

// Cursor is a struct for iterating through the entries
// in the key value store.
type Cursor struct {
	cursor *bolt.Cursor

	// previously seeked key/value
	key, value []byte

	config kv.CursorConfig
	closed bool
}

// Close sets the closed to closed
func (c *Cursor) Close() error {
	c.closed = true

	return nil
}

// Seek seeks for the first key that matches the prefix provided.
func (c *Cursor) Seek(prefix []byte) ([]byte, []byte) {
	if c.closed {
		return nil, nil
	}
	k, v := c.cursor.Seek(prefix)
	if len(k) == 0 && len(v) == 0 {
		return nil, nil
	}
	return k, v
}

// First retrieves the first key value pair in the bucket.
func (c *Cursor) First() ([]byte, []byte) {
	if c.closed {
		return nil, nil
	}
	k, v := c.cursor.First()
	if len(k) == 0 && len(v) == 0 {
		return nil, nil
	}
	return k, v
}

// Last retrieves the last key value pair in the bucket.
func (c *Cursor) Last() ([]byte, []byte) {
	if c.closed {
		return nil, nil
	}
	k, v := c.cursor.Last()
	if len(k) == 0 && len(v) == 0 {
		return nil, nil
	}
	return k, v
}

// Next retrieves the next key in the bucket.
func (c *Cursor) Next() (k []byte, v []byte) {
	if c.closed || (c.key != nil && c.missingPrefix(c.key)) {
		return nil, nil
	}
	// get and unset previously seeked values if they exist
	k, v, c.key, c.value = c.key, c.value, nil, nil
	if len(k) > 0 || len(v) > 0 {
		return
	}

	next := c.cursor.Next
	if c.config.Direction == kv.CursorDescending {
		next = c.cursor.Prev
	}

	k, v = next()
	if (len(k) == 0 && len(v) == 0) || c.missingPrefix(k) {
		return nil, nil
	}
	return k, v
}

// Prev retrieves the previous key in the bucket.
func (c *Cursor) Prev() (k []byte, v []byte) {
	if c.closed || (c.key != nil && c.missingPrefix(c.key)) {
		return nil, nil
	}

	// get and unset previously seeked values if they exist
	k, v, c.key, c.value = c.key, c.value, nil, nil
	if len(k) > 0 && len(v) > 0 {
		return
	}

	prev := c.cursor.Prev
	if c.config.Direction == kv.CursorDescending {
		prev = c.cursor.Next
	}

	k, v = prev()
	if (len(k) == 0 && len(v) == 0) || c.missingPrefix(k) {
		return nil, nil
	}
	return k, v
}

func (c *Cursor) missingPrefix(key []byte) bool {
	return c.config.Prefix != nil && !bytes.HasPrefix(key, c.config.Prefix)
}

// Err always returns nil as nothing can go wrong™ during iteration
func (c *Cursor) Err() error {
	return nil
}
