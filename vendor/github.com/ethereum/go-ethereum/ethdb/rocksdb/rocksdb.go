package rocksdb

import (
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/linxGnu/grocksdb"
	"strings"
	"sync"
	"time"
)

const (
	//degradationWarnInterval  = time.Minute
	metricsGatheringInterval = 3 * time.Second
	minCache                 = 16
	minHandles               = 16
)

type Database struct {
	fn string
	db *grocksdb.DB

	cfHandles map[string]*grocksdb.ColumnFamilyHandle // 保存列族句柄

	options   *grocksdb.Options
	readOpts  *grocksdb.ReadOptions
	writeOpts *grocksdb.WriteOptions

	quitLock sync.Mutex
	quitChan chan chan error

	log log.Logger

	compTimeMeter  metrics.Meter
	compReadMeter  metrics.Meter
	compWriteMeter metrics.Meter
	diskSizeGauge  metrics.Gauge
	diskReadMeter  metrics.Meter
	diskWriteMeter metrics.Meter
}

// New initializes a new RocksDB instance with the default column family
func New(path string, cache int, handles int, namespace string) (*Database, error) {
	return NewCF(path, cache, handles, namespace, []string{"default"})
}

// NewCF initializes a new RocksDB instance
func NewCF(path string, cache int, handles int, namespace string, columnFamilies []string) (*Database, error) {
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}

	if len(columnFamilies) == 0 {
		columnFamilies = []string{"default"}
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(handles)
	opts.SetCreateIfMissingColumnFamilies(true)
	//opts.EnableStatistics()

	// Correct way to set the cache using BlockBasedTableOptions
	tableOpts := grocksdb.NewDefaultBlockBasedTableOptions()
	cache2 := grocksdb.NewLRUCache(uint64(cache * 1024 * 1024))
	tableOpts.SetBlockCache(cache2)
	opts.SetBlockBasedTableFactory(tableOpts)

	cfOptions := make([]*grocksdb.Options, len(columnFamilies))
	cfHandles := make([]*grocksdb.ColumnFamilyHandle, len(columnFamilies))
	for i := range columnFamilies {
		cfOptions[i] = grocksdb.NewDefaultOptions()
		cfOptions[i].EnableBlobFiles(true)
		cfOptions[i].SetMinBlobSize(128)
		cfOptions[i].SetBlobCompressionType(grocksdb.NoCompression)
		cfOptions[i].EnableBlobGC(true)
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, path, columnFamilies, cfOptions)
	if err != nil {
		return nil, err
	}

	cfHandleMap := make(map[string]*grocksdb.ColumnFamilyHandle)
	for i, cf := range columnFamilies {
		cfHandleMap[cf] = cfHandles[i]
	}

	logger := log.New("database", path)

	ldb := &Database{
		fn:        path,
		db:        db,
		cfHandles: cfHandleMap,
		options:   opts,
		readOpts:  grocksdb.NewDefaultReadOptions(),
		writeOpts: grocksdb.NewDefaultWriteOptions(),
		quitChan:  make(chan chan error),
		log:       logger,
	}

	// Initialize metrics
	ldb.compTimeMeter = metrics.NewRegisteredMeter(namespace+"compact/time", nil)
	ldb.compReadMeter = metrics.NewRegisteredMeter(namespace+"compact/input", nil)
	ldb.compWriteMeter = metrics.NewRegisteredMeter(namespace+"compact/output", nil)
	ldb.diskSizeGauge = metrics.NewRegisteredGauge(namespace+"disk/size", nil)
	ldb.diskReadMeter = metrics.NewRegisteredMeter(namespace+"disk/read", nil)
	ldb.diskWriteMeter = metrics.NewRegisteredMeter(namespace+"disk/write", nil)

	go ldb.meter(metricsGatheringInterval)

	return ldb, nil
}

// Close gracefully closes the RocksDB instance
func (r *Database) Close() error {
	r.quitLock.Lock()
	defer r.quitLock.Unlock()

	if r.quitChan != nil {
		errc := make(chan error)
		r.quitChan <- errc
		if err := <-errc; err != nil {
			r.log.Error("Metrics collection failed", "err", err)
		}
		r.quitChan = nil
	}
	r.options.Destroy()
	r.db.Close()
	return nil
}

// Has in RocksDB
func (r *Database) Has(key []byte) (bool, error) {
	slice, err := r.db.Get(r.readOpts, key)
	if err != nil {
		return false, err
	}
	defer slice.Free()

	// 在 slice.Free() 前判断数据是否存在
	valueExists := slice.Exists()
	return valueExists, nil
}

// Get retrieves the value for a given key
func (r *Database) Get(key []byte) ([]byte, error) {
	slice, err := r.db.Get(r.readOpts, key)
	if err != nil {
		return nil, err
	}
	defer slice.Free()

	// 复制 slice.Data() 到新的缓冲区
	value := make([]byte, slice.Size())
	copy(value, slice.Data())

	return value, nil
}

// Put writes a key-value pair into RocksDB
func (r *Database) Put(key []byte, value []byte) error {
	return r.db.Put(r.writeOpts, key, value)
}

// Delete removes the key from RocksDB
func (r *Database) Delete(key []byte) error {
	return r.db.Delete(r.writeOpts, key)
}

// HasCF checks if a key exists in the specified column family
func (r *Database) HasCF(cf string, key []byte) (bool, error) {
	handle, ok := r.cfHandles[cf]
	if !ok {
		return false, fmt.Errorf("column family %s not found", cf)
	}
	slice, err := r.db.GetCF(r.readOpts, handle, key)
	if err != nil {
		return false, err
	}
	defer slice.Free()

	valueExists := slice.Exists()
	return valueExists, nil
}

// GetCF retrieves the value for a given key from the specified column family
func (r *Database) GetCF(cf string, key []byte) ([]byte, error) {
	handle, ok := r.cfHandles[cf]
	if !ok {
		return nil, fmt.Errorf("column family %s not found", cf)
	}
	slice, err := r.db.GetCF(r.readOpts, handle, key)
	if err != nil {
		return nil, err
	}
	defer slice.Free()

	value := make([]byte, slice.Size())
	copy(value, slice.Data())

	return value, nil
}

// PutCF writes a key-value pair into the specified column family
func (r *Database) PutCF(cf string, key []byte, value []byte) error {
	handle, ok := r.cfHandles[cf]
	if !ok {
		return fmt.Errorf("column family %s not found", cf)
	}
	return r.db.PutCF(r.writeOpts, handle, key, value)
}

// DeleteCF removes the key from the specified column family
func (r *Database) DeleteCF(cf string, key []byte) error {
	handle, ok := r.cfHandles[cf]
	if !ok {
		return fmt.Errorf("column family %s not found", cf)
	}
	return r.db.DeleteCF(r.writeOpts, handle, key)
}

// Iterator wraps the RocksDB iterator to implement ethdb.Iterator.
type Iterator struct {
	it  *grocksdb.Iterator
	err error
}

// NewIteratorCF creates a new iterator for a prefix and start position in the specified column family.
func (r *Database) NewIteratorCF(cf string, prefix []byte, start []byte) ethdb.Iterator {
	// Define an upper bound to limit the iterator to the prefix range.
	upperBound := append(prefix[:len(prefix)-1], prefix[len(prefix)-1]+1)

	itOpts := grocksdb.NewDefaultReadOptions()
	itOpts.SetPrefixSameAsStart(true)
	itOpts.SetIterateUpperBound(upperBound) // Sets an upper bound for iteration

	handle, ok := r.cfHandles[cf]
	if !ok {
		return &Iterator{err: fmt.Errorf("column family %s not found", cf)}
	}

	// Create the iterator from RocksDB
	it := r.db.NewIteratorCF(itOpts, handle)
	it.Seek(start)

	return &Iterator{
		it: it,
	}
}

// NewIterator creates a new iterator for a prefix and start position in the default column family.
func (r *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	// Define an upper bound to limit the iterator to the prefix range.
	upperBound := append(prefix[:len(prefix)-1], prefix[len(prefix)-1]+1)

	itOpts := grocksdb.NewDefaultReadOptions()
	itOpts.SetPrefixSameAsStart(true)
	itOpts.SetIterateUpperBound(upperBound) // Sets an upper bound for iteration

	handle, ok := r.cfHandles["default"]
	if !ok {
		return &Iterator{err: fmt.Errorf("default column family not found")}
	}

	// Create the iterator from RocksDB
	it := r.db.NewIteratorCF(itOpts, handle)
	it.Seek(start)

	return &Iterator{
		it: it,
	}
}

// Next moves the iterator to the next key/value pair.
func (it *Iterator) Next() bool {
	it.it.Next()
	return it.it.Valid()
}

// Error returns any accumulated error.
func (it *Iterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.it.Err()
}

// Key returns the current key at the iterator's position.
func (it *Iterator) Key() []byte {
	return it.it.Key().Data()
}

// Value returns the current value at the iterator's position.
func (it *Iterator) Value() []byte {
	return it.it.Value().Data()
}

// Release closes the iterator and releases any associated resources.
func (it *Iterator) Release() {
	it.it.Close()
}

func (r *Database) Stat(property string) (string, error) {
	// Flush all column families to ensure all writes are persisted before getting stats
	for _, cfHandle := range r.cfHandles {
		if err := r.db.FlushCF(cfHandle, grocksdb.NewDefaultFlushOptions()); err != nil {
			return "", fmt.Errorf("failed to flush column family: %v", err)
		}
	}
	var result strings.Builder
	for cfName, cfHandle := range r.cfHandles {
		stat := r.db.GetPropertyCF(property, cfHandle)
		result.WriteString(fmt.Sprintf("%s: %s\n", cfName, stat))
	}
	return result.String(), nil
}

// Compact compacts the database for a specific range
func (r *Database) Compact(start, limit []byte) error {
	r.db.CompactRange(grocksdb.Range{Start: start, Limit: limit})
	return nil
}

// Path returns the database file path
func (r *Database) Path() string {
	return r.fn
}

// meter function collects metrics for compaction and I/O stats
func (r *Database) meter(refresh time.Duration) {
	// Create the counters to store current and previous compaction values
	compactions := make([][]float64, 2)
	for i := 0; i < 2; i++ {
		compactions[i] = make([]float64, 4)
	}

	// Create storage for iostats
	var iostats [2]float64

	// Create storage and warning log tracer for write delay
	//var (
	//	delaystats      [2]int64
	//	lastWritePaused time.Time
	//)

	var (
		errc chan error
		merr error
	)

	timer := time.NewTimer(refresh)
	defer timer.Stop()

	// Iterate and collect the stats periodically
	for i := 1; errc == nil && merr == nil; i++ {
		// Retrieve the database stats
		stats := r.db.GetProperty("rocksdb.stats")

		r.log.Info("RocksDB Stats", "stats", stats)

		// Retrieve write stall statistics
		writeStallStats := r.db.GetProperty("rocksdb.write-stall")

		r.log.Info("Write Stall Stats", "writeStallStats", writeStallStats)

		// Retrieve the database I/O stats
		ioStats := r.db.GetProperty("rocksdb.iostats")

		// Example parsing of read/write MB stats
		var nRead, nWrite float64
		parts := strings.Split(ioStats, " ")
		if len(parts) < 2 {
			r.log.Error("Bad syntax in iostats", "ioStats", ioStats)
			merr = fmt.Errorf("bad syntax in iostats: %s", ioStats)
			continue
		}
		if n, err := fmt.Sscanf(parts[0], "Read(MB):%f", &nRead); n != 1 || err != nil {
			r.log.Error("Bad syntax of read entry", "entry", parts[0])
			merr = err
			continue
		}
		if n, err := fmt.Sscanf(parts[1], "Write(MB):%f", &nWrite); n != 1 || err != nil {
			r.log.Error("Bad syntax of write entry", "entry", parts[1])
			merr = err
			continue
		}
		if r.diskReadMeter != nil {
			r.diskReadMeter.Mark(int64((nRead - iostats[0]) * 1024 * 1024))
		}
		if r.diskWriteMeter != nil {
			r.diskWriteMeter.Mark(int64((nWrite - iostats[1]) * 1024 * 1024))
		}
		iostats[0], iostats[1] = nRead, nWrite

		// Sleep a bit, then repeat the stats collection
		select {
		case errc = <-r.quitChan:
			// Quit requested, stop collecting stats
		case <-timer.C:
			timer.Reset(refresh)
			// Timeout, gather new stats
		}
	}

	if errc == nil {
		errc = <-r.quitChan
	}
	errc <- merr
}

func (r *Database) NewBatch() ethdb.Batch {
	return &batch{
		db: r.db,
		b:  grocksdb.NewWriteBatch(),
	}
}

func (r *Database) NewBatchCF(cf string) ethdb.Batch {
	handle, ok := r.cfHandles[cf]
	if !ok {
		return nil
	}
	return &batch{
		db:     r.db,
		b:      grocksdb.NewWriteBatch(),
		handle: handle,
	}
}

// operation represents a single operation in the batch (Put or Delete).
type operation struct {
	opType int // 1 for Put, 2 for Delete
	key    []byte
	value  []byte // Only for Put operations, nil for Delete
}

// batch is a write-only RocksDB batch that commits changes to its host database.
type batch struct {
	db     *grocksdb.DB
	b      *grocksdb.WriteBatch
	handle *grocksdb.ColumnFamilyHandle // Optional column family handle
	size   int
	ops    []operation // Track the operations for replay
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {

	b.b.Put(key, value)

	b.size += len(value) + len(key)                                     // Track size as total key-value length
	b.ops = append(b.ops, operation{opType: 1, key: key, value: value}) // Track operation
	return nil
}

// PutCF inserts the given value into the batch for the specified column family.
func (b *batch) PutCF(cf string, key []byte, value []byte) error {
	if b.handle != nil {
		b.b.PutCF(b.handle, key, value)
	} else {
		return fmt.Errorf("column family handle is not set")
	}
	b.size += len(value) + len(key)                                     // Track size as total key-value length
	b.ops = append(b.ops, operation{opType: 1, key: key, value: value}) // Track operation
	return nil
}

// Delete queues a key deletion for later committing.
func (b *batch) Delete(key []byte) error {
	if b.handle != nil {
		b.b.DeleteCF(b.handle, key)
	} else {
		b.b.Delete(key)
	}
	b.size += len(key)
	b.ops = append(b.ops, operation{opType: 2, key: key}) // Track operation
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk (atomic write).
func (b *batch) Write() error {
	writeOpts := grocksdb.NewDefaultWriteOptions() // Create WriteOptions
	defer writeOpts.Destroy()                      // Ensure options are cleaned up after use
	err := b.db.Write(writeOpts, b.b)              // Pass both WriteBatch and WriteOptions
	return err
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.b.Clear()
	b.size = 0
	b.ops = nil // Reset the operation log as well
}

// Replay replays the batch contents to another writer (replayer logic).
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	handler := &replayer{writer: w}
	for _, op := range b.ops {
		if handler.failure != nil {
			return handler.failure // Stop if there's an error
		}
		if op.opType == 1 {
			handler.Put(op.key, op.value) // Replay Put operations
		} else if op.opType == 2 {
			handler.Delete(op.key) // Replay Delete operations
		}
	}
	return nil
}

// replayer is a wrapper that helps in replaying batch operations to another KeyValueWriter.
type replayer struct {
	writer  ethdb.KeyValueWriter
	failure error
}

// Put inserts a key-value pair to the destination writer during replay.
func (r *replayer) Put(key, value []byte) {
	if r.failure != nil {
		return // Stop if a failure has occurred
	}
	r.failure = r.writer.Put(key, value)
}

// Delete removes the key from the destination writer during replay.
func (r *replayer) Delete(key []byte) {
	if r.failure != nil {
		return // Stop if a failure has occurred
	}
	r.failure = r.writer.Delete(key)
}
