package eds

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/shard"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/multiformats/go-multihash"
)

const invertedIndexPath = "/inverted_index/"

// ErrNotFoundInIndex is returned instead of ErrNotFound if the multihash doesn't exist in the index
var ErrNotFoundInIndex = errors.New("does not exist in index")

// pebbleDatastore is a wrapper around PebbleDB that implements the Datastore interface.
type pebbleDatastore struct {
	db    *pebble.DB
	mu    sync.RWMutex
	closed bool
}

func newPebbleDatastore(path string) (*pebbleDatastore, error) {
	// cache := pebble.NewCache(1026)
	// // compress := pebble.ZstdCompression
	// filter := bloom.FilterPolicy(10)
	// lopts := make([]pebble.LevelOptions, 0)
	// opt := pebble.LevelOptions{
	// 		// Compression:    compress,
	// 		// BlockSize:      cfg.BlockSize,
	// 		// TargetFileSize: int64(cfg.TargetFileSizeBase),
	// 		FilterPolicy:   filter,
	// 	}

	// opt.EnsureDefaults()
	// lopts = append(lopts, opt)

	// opts := &pebble.Options{
	// 	MaxManifestFileSize: 64 << 20,
	// 	MemTableSize:        64 << 20,
	// 	Cache:               cache, // 1gb
	// 	Levels: lopts,
	// 	// Compression: compress,
	// 	// MaxConcurrentCompactions:    3,
	// 	// MemTableStopWritesThreshold: 4,
	// }


	cache := 512
	handles := 4096
	memTableLimit := 2
	memTableSize := cache * 1024 * 1024 / 2 / memTableLimit
	// compress := pebble.ZstdCompression //
	opt := &pebble.Options{
		// Pebble has a single combined cache area and the write
		// buffers are taken from this too. Assign all available
		// memory allowance for cache.
		Cache:        pebble.NewCache(int64(cache * 1024 * 1024)),
		MaxOpenFiles: handles,

		// The size of memory table(as well as the write buffer).
		// Note, there may have more than two memory tables in the system.
		MemTableSize: uint64(memTableSize),

		// MemTableStopWritesThreshold places a hard limit on the size
		// of the existent MemTables(including the frozen one).
		// Note, this must be the number of tables not the size of all memtables
		// according to https://github.com/cockroachdb/pebble/blob/master/options.go#L738-L742
		// and to https://github.com/cockroachdb/pebble/blob/master/db.go#L1892-L1903.
		MemTableStopWritesThreshold: memTableLimit,

		// The default compaction concurrency(1 thread),
		// Here use all available CPUs for faster compaction.
		MaxConcurrentCompactions: runtime.NumCPU,

		// Per-level options. Options for at least one level must be specified. The
		// options for the last level are used for all subsequent levels.
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			// {Compression: compress},
		},
		// ReadOnly: readonly,
		// EventListener: &pebble.EventListener{
		// 	CompactionBegin: db.onCompactionBegin,
		// 	CompactionEnd:   db.onCompactionEnd,
		// 	WriteStallBegin: db.onWriteStallBegin,
		// 	WriteStallEnd:   db.onWriteStallEnd,
		// },
		// Logger: panicLogger{}, // TODO(karalabe): Delete when this is upstreamed in Pebble
	}
	// Disable seek compaction explicitly. Check https://github.com/ethereum/go-ethereum/pull/20130
	// for more details.
	opt.Experimental.ReadSamplingMultiplier = -1

	db, err := pebble.Open(path, opt)
	if err != nil {
		return nil, err
	}
	return &pebbleDatastore{db: db}, nil
}

func (p *pebbleDatastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errors.New("pebble: closed")
	}
	return p.db.Set([]byte(key.String()), value, pebble.Sync)
}

func (p *pebbleDatastore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, errors.New("pebble: closed")
	}
	value, closer, err := p.db.Get([]byte(key.String()))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	return value, nil
}

func (p *pebbleDatastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return false, errors.New("pebble: closed")
	}
	_, closer, err := p.db.Get([]byte(key.String()))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	defer closer.Close()
	return true, nil
}

func (p *pebbleDatastore) Delete(ctx context.Context, key ds.Key) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errors.New("pebble: closed")
	}
	return p.db.Delete([]byte(key.String()), pebble.Sync)
}

func (p *pebbleDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	return nil, errors.New("not implemented")
}

func (p *pebbleDatastore) Batch(ctx context.Context) (ds.Batch, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, errors.New("pebble: closed")
	}
	return &pebbleBatch{db: p.db, batch: p.db.NewBatch()}, nil
}

func (p *pebbleDatastore) GetSize(ctx context.Context, key ds.Key) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return -1, errors.New("pebble: closed")
	}
	value, closer, err := p.db.Get([]byte(key.String()))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return -1, ds.ErrNotFound
		}
		return -1, err
	}
	defer closer.Close()
	return len(value), nil
}

func (p *pebbleDatastore) Sync(ctx context.Context, prefix ds.Key) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errors.New("pebble: closed")
	}
	return p.db.Flush()
}

func (p *pebbleDatastore) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("pebble: already closed")
	}
	p.closed = true
	return p.db.Close()
}

type pebbleBatch struct {
	db    *pebble.DB
	batch *pebble.Batch
}

func (b *pebbleBatch) Put(ctx context.Context, key ds.Key, value []byte) error {
	return b.batch.Set([]byte(key.String()), value, nil)
}

func (b *pebbleBatch) Delete(ctx context.Context, key ds.Key) error {
	return b.batch.Delete([]byte(key.String()), nil)
}

func (b *pebbleBatch) Commit(ctx context.Context) error {
	return b.batch.Commit(pebble.Sync)
}

func (b *pebbleBatch) Cancel() error {
	return b.batch.Close()
}

// simpleInvertedIndex is an inverted index that only stores a single shard key per multihash. Its
// implementation is modified from the default upstream implementation in dagstore/index.
type simpleInvertedIndex struct {
	ds ds.Batching
}

// newSimpleInvertedIndex returns a new inverted index that only stores a single shard key per
// multihash.
func newSimpleInvertedIndex(storePath string) (*simpleInvertedIndex, error) {
	ds, err := newPebbleDatastore(filepath.Join(storePath, invertedIndexPath))
	if err != nil {
		return nil, fmt.Errorf("can't open Pebble Datastore: %w", err)
	}

	return &simpleInvertedIndex{ds: ds}, nil
}

func (s *simpleInvertedIndex) AddMultihashesForShard(
	ctx context.Context,
	mhIter index.MultihashIterator,
	sk shard.Key,
) error {
	batch, err := s.ds.Batch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create ds batch: %w", err)
	}

	err = mhIter.ForEach(func(mh multihash.Multihash) error {
		key := ds.NewKey(string(mh))
		if err := batch.Put(ctx, key, []byte(sk.String())); err != nil {
			return fmt.Errorf("failed to put mh=%s, err=%w", mh, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to add index entry: %w", err)
	}

	if err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}
	return nil
}

func (s *simpleInvertedIndex) GetShardsForMultihash(ctx context.Context, mh multihash.Multihash) ([]shard.Key, error) {
	key := ds.NewKey(string(mh))
	sbz, err := s.ds.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return nil, ErrNotFoundInIndex
		}
		return nil, err
	}

	return []shard.Key{shard.KeyFromString(string(sbz))}, nil
}

func (s *simpleInvertedIndex) close() error {
	return s.ds.Close()
}
