package nodebuilder

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	"github.com/gofrs/flock"
	"github.com/ipfs/go-datastore"
	dspebble "github.com/ipfs/go-ds-pebble"
	"github.com/cockroachdb/pebble"
	"github.com/mitchellh/go-homedir"

	"github.com/celestiaorg/celestia-node/libs/keystore"
	nodemod "github.com/celestiaorg/celestia-node/nodebuilder/node"
	"github.com/celestiaorg/celestia-node/nodebuilder/p2p"
	"github.com/cockroachdb/pebble/bloom"
	"runtime"
)

var (
	// ErrOpened is thrown on attempt to open already open/in-use Store.
	ErrOpened = errors.New("node: store is in use")
	// ErrNotInited is thrown on attempt to open Store without initialization.
	ErrNotInited = errors.New("node: store is not initialized")
	// ErrNoOpenStore is thrown when no opened Store is found, indicating that no node is running.
	ErrNoOpenStore = errors.New("no opened Node Store found (no node is running)")
)

// Store encapsulates storage for the Node. Basically, it is the Store of all Stores.
// It provides access for the Node data stored in root directory e.g. '~/.celestia'.
type Store interface {
	// Path reports the FileSystem path of Store.
	Path() string

	// Keystore provides a Keystore to access keys.
	Keystore() (keystore.Keystore, error)

	// Datastore provides a Datastore - a KV store for arbitrary data to be stored on disk.
	Datastore() (datastore.Batching, error)

	// Config loads the stored Node config.
	Config() (*Config, error)

	// PutConfig alters the stored Node config.
	PutConfig(*Config) error

	// Close closes the Store freeing up acquired resources and locks.
	Close() error
}

// OpenStore creates new FS Store under the given 'path'.
// To be opened the Store must be initialized first, otherwise ErrNotInited is thrown.
// OpenStore takes a file Lock on directory, hence only one Store can be opened at a time under the
// given 'path', otherwise ErrOpened is thrown.
func OpenStore(path string, ring keyring.Keyring) (Store, error) {
	path, err := storePath(path)
	if err != nil {
		return nil, err
	}

	flk := flock.New(lockPath(path))
	ok, err := flk.TryLock()
	if err != nil {
		return nil, fmt.Errorf("locking file: %w", err)
	}
	if !ok {
		return nil, ErrOpened
	}

	if !IsInit(path) {
		err := errors.Join(ErrNotInited, flk.Unlock())
		return nil, err
	}

	ks, err := keystore.NewFSKeystore(keysPath(path), ring)
	if err != nil {
		err = errors.Join(err, flk.Unlock())
		return nil, err
	}

	return &fsStore{
		path:    path,
		dirLock: flk,
		keys:    ks,
	}, nil
}

func (f *fsStore) Path() string {
	return f.path
}

func (f *fsStore) Config() (*Config, error) {
	cfg, err := LoadConfig(configPath(f.path))
	if err != nil {
		return nil, fmt.Errorf("node: can't load Config: %w", err)
	}

	return cfg, nil
}

func (f *fsStore) PutConfig(cfg *Config) error {
	err := SaveConfig(configPath(f.path), cfg)
	if err != nil {
		return fmt.Errorf("node: can't save Config: %w", err)
	}

	return nil
}

func (f *fsStore) Keystore() (_ keystore.Keystore, err error) {
	if f.keys == nil {
		return nil, fmt.Errorf("node: no Keystore found")
	}
	return f.keys, nil
}

func (f *fsStore) Datastore() (datastore.Batching, error) {
	f.dataMu.Lock()
	defer f.dataMu.Unlock()
	if f.data != nil {
		return f.data, nil
	}

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

	ds, err := dspebble.NewDatastore(dataPath(f.path), opt)
	if err != nil {
		return nil, fmt.Errorf("node: can't open Pebble Datastore: %w", err)
	}

	f.data = ds
	return ds, nil
}

func (f *fsStore) Close() (err error) {
	err = errors.Join(err, f.dirLock.Close())
	f.dataMu.Lock()
	if f.data != nil {
		err = errors.Join(err, f.data.Close())
	}
	f.dataMu.Unlock()
	return
}

type fsStore struct {
	path string

	dataMu  sync.Mutex
	data    datastore.Batching
	keys    keystore.Keystore
	dirLock *flock.Flock // protects directory
}

// DiscoverOpened finds a path of an opened Node Store and returns its path.
// If multiple nodes are running, it only returns the path of the first found node.
// Network is favored over node type.
//
// Network preference order: Mainnet, Mocha, Arabica, Private, Custom
// Type preference order: Bridge, Full, Light
func DiscoverOpened() (string, error) {
	defaultNetwork := p2p.GetNetworks()
	nodeTypes := nodemod.GetTypes()

	for _, n := range defaultNetwork {
		for _, tp := range nodeTypes {
			path, err := DefaultNodeStorePath(tp.String(), n.String())
			if err != nil {
				return "", err
			}

			ok, _ := IsOpened(path)
			if ok {
				return path, nil
			}
		}
	}

	return "", ErrNoOpenStore
}

// DefaultNodeStorePath constructs the default node store path using the given
// node type and network.
var DefaultNodeStorePath = func(tp, network string) (string, error) {
	home := os.Getenv("CELESTIA_HOME")

	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			return "", err
		}
	}
	if network == p2p.Mainnet.String() {
		return fmt.Sprintf("%s/.celestia-%s", home, strings.ToLower(tp)), nil
	}
	// only include network name in path for testnets and custom networks
	return fmt.Sprintf(
		"%s/.celestia-%s-%s",
		home,
		strings.ToLower(tp),
		strings.ToLower(network),
	), nil
}

// IsOpened checks if the Store is opened in a directory by checking its file lock.
func IsOpened(path string) (bool, error) {
	flk := flock.New(lockPath(path))
	ok, err := flk.TryLock()
	if err != nil {
		return false, fmt.Errorf("locking file: %w", err)
	}

	err = flk.Unlock()
	if err != nil {
		return false, fmt.Errorf("unlocking file: %w", err)
	}

	return !ok, nil
}

func storePath(path string) (string, error) {
	return homedir.Expand(filepath.Clean(path))
}

func configPath(base string) string {
	return filepath.Join(base, "config.toml")
}

func lockPath(base string) string {
	return filepath.Join(base, ".lock")
}

func keysPath(base string) string {
	return filepath.Join(base, "keys")
}

func blocksPath(base string) string {
	return filepath.Join(base, "blocks")
}

func transientsPath(base string) string {
	// we don't actually use the transients directory anymore, but it could be populated from previous
	// versions.
	return filepath.Join(base, "transients")
}

func indexPath(base string) string {
	return filepath.Join(base, "index")
}

func dataPath(base string) string {
	return filepath.Join(base, "data")
}
