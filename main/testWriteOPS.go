package main

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/ethereum/go-ethereum/params"
)

// BlockChainContext implements the ChainContext interface
type BlockChainContext struct {
	db      ethdb.Database
	engine  consensus.Engine
	headers map[common.Hash]*types.Header
}

var transactionTimes []time.Duration

func (bc *BlockChainContext) Engine() consensus.Engine {
	return bc.engine
}

func (bc *BlockChainContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	if header, ok := bc.headers[hash]; ok {
		return header
	}
	return rawdb.ReadHeader(bc.db, hash, number)
}

func main() {
	processBlocks()
	averageTime := calculateAverageTransactionTime()
	fmt.Printf("Average transaction execution time: %v\n", averageTime)

	saveDurationsToJSON("durations_25w_V4.json")

}

func processBlocks() {
	//ancientDbPath := "/mnt/sda/geth1.9.25data/geth/chaindata"
	ancientDbPath := "/mnt/sdb/gethdata/geth/chaindata"

	ancientPath := ancientDbPath + "/ancient"
	//newDbPath := "/mnt/sda/geth1.9.25data/geth/leveldb200W"
	newDbPath := "/mnt/sda/geth1.9.25data/geth/rocksdb_25w_V6"

	// Open the ancient database
	ancientDb, err := rawdb.NewLevelDBDatabaseWithFreezer(ancientDbPath, 16, 1, ancientPath, "")
	if err != nil {
		log.Fatalf("Failed to open ancient database: %v", err)
	}
	defer ancientDb.Close()

	newDb, err := rawdb.NewRocksDBDatabase2(newDbPath, 16, 16, "")
	//newDb, err := rawdb.NewRocksDBDatabase(newDbPath, 16, 16, "")
	//newDb, err := rawdb.NewLevelDBDatabase(newDbPath, 16, 1, "")
	if err != nil {
		log.Fatalf("Failed to create new database: %v", err)
	}
	stateDB := state.NewDatabase(newDb)
	defer newDb.Close()

	// Create a new BlockChain instance
	chainConfig := params.MainnetChainConfig

	engine := ethash.NewFullFaker()

	// Create a new BlockChainContext instance
	blockChainContext := &BlockChainContext{
		db:      newDb,
		engine:  engine,
		headers: make(map[common.Hash]*types.Header),
	}

	// Use the empty root (genesis state) for the first block
	var currentRoot *trie.NewKey = nil
	//Root := common.Hash{}

	//currentRoot, err = loadCurrentRoot("currentRoot.json")
	//if err != nil {
	//	log.Println("No previous state found, starting from genesis")
	//	currentRoot = nil
	//}

	// Iterate over blocks and apply transactions
	for blockNumber := uint64(1); blockNumber < uint64(250001); blockNumber++ {
		//newRoot, err := processBlock(blockNumber, ancientDb, newDb, stateDB, chainConfig, blockChainContext, Root)
		_, err := processBlock(blockNumber, ancientDb, newDb, stateDB, chainConfig, blockChainContext, &currentRoot)
		if err != nil {
			if err.Error() == "no more blocks" {
				break
			}
			log.Fatalf("Error processing block %d: %v", blockNumber, err)
		}

		//Root = newRoot

		if blockNumber%100000 == 0 {
			time.Sleep(time.Second * 5)
			runtime.GC()
			debug.FreeOSMemory()
			time.Sleep(time.Second * 5)
		}
	}

	ops := float64(rawdb.BlocksProcessed) / rawdb.TotalDuration.Seconds()

	fmt.Printf("Processed %d blocks in %.2f seconds, %.2f OPS\n", rawdb.BlocksProcessed, rawdb.TotalDuration.Seconds(), ops)
	//showStat2(newDb)
	showRocksdbStat3(newDb)

}

// func processBlock(blockNumber uint64, ancientDb, newDb ethdb.Database, stateDB state.Database, chainConfig *params.ChainConfig, blockChainContext *BlockChainContext, currentRoot **trie.NewKey) error {
// func processBlock(blockNumber uint64, ancientDb, newDb ethdb.Database, stateDB state.Database, chainConfig *params.ChainConfig, blockChainContext *BlockChainContext, currentRoot common.Hash) (common.Hash, error) {
func processBlock(blockNumber uint64, ancientDb, newDb ethdb.Database, stateDB state.Database, chainConfig *params.ChainConfig, blockChainContext *BlockChainContext, currentRoot **trie.NewKey) (common.Hash, error) {
	blockHash := rawdb.ReadCanonicalHash(ancientDb, blockNumber)
	if blockHash == (common.Hash{}) {
		return common.Hash{}, fmt.Errorf("no more blocks")
	}

	header := rawdb.ReadHeader(ancientDb, blockHash, blockNumber)
	if header == nil {
		return common.Hash{}, fmt.Errorf("failed to read header for block %d", blockNumber)
	}

	body := rawdb.ReadBody(ancientDb, blockHash, blockNumber)
	if body == nil {
		return common.Hash{}, fmt.Errorf("failed to read body for block %d", blockNumber)
	}

	receipts := rawdb.ReadReceipts(ancientDb, blockHash, blockNumber, chainConfig)
	if receipts == nil {
		return common.Hash{}, fmt.Errorf("failed to read receipts for block %d", blockNumber)
	}

	td := rawdb.ReadTd(ancientDb, blockHash, blockNumber)
	if td == nil {
		return common.Hash{}, fmt.Errorf("failed to read total difficulty for block %d", blockNumber)
	}
	rawdb.WriteTd2(newDb, blockHash, blockNumber, td)
	rawdb.WriteBody2(newDb, blockHash, blockNumber, body)
	rawdb.WriteHeader2(newDb, header)
	rawdb.WriteReceipts2(newDb, blockHash, blockNumber, receipts)
	rawdb.WriteCanonicalHash2(newDb, blockHash, blockNumber)

	rawdb.WriteHeadBlockHash2(newDb, blockHash)
	rawdb.WriteHeadHeaderHash2(newDb, blockHash)
	rawdb.WriteHeadFastBlockHash2(newDb, blockHash)

	//rawdb.WriteTd(newDb, blockHash, blockNumber, td)
	//rawdb.WriteBody(newDb, blockHash, blockNumber, body)
	//rawdb.WriteHeader(newDb, header)
	//rawdb.WriteReceipts(newDb, blockHash, blockNumber, receipts)
	//rawdb.WriteCanonicalHash(newDb, blockHash, blockNumber)
	//
	//rawdb.WriteHeadBlockHash(newDb, blockHash)
	//rawdb.WriteHeadHeaderHash(newDb, blockHash)
	//rawdb.WriteHeadFastBlockHash(newDb, blockHash)

	if len(body.Transactions) > 0 {
		//newState, err := state.New(currentRoot, stateDB, nil)
		newState, err := state.New2(*currentRoot, stateDB, nil)
		if err != nil {
			return common.Hash{}, fmt.Errorf("failed to create state: %v", err)
		}

		gasPool := new(core.GasPool).AddGas(header.GasLimit)

		for _, tx := range body.Transactions {
			start := time.Now()
			receipt, err := core.ApplyTransaction(chainConfig, blockChainContext, &header.Coinbase, gasPool, newState, header, tx, new(uint64), vm.Config{})
			elapsed := time.Since(start)
			transactionTimes = append(transactionTimes, elapsed)

			if err != nil {
				return common.Hash{}, fmt.Errorf("failed to apply transaction: %v", err)
			}
			if blockNumber%10000 == 0 {
				fmt.Printf("Applied transaction: %s, Receipt: %v\n", tx.Hash().Hex(), receipt)
			}
		}

		root, err := newState.Commit(true)
		if err != nil {
			return common.Hash{}, fmt.Errorf("failed to commit state: %v", err)
		}

		// commit2： S_MPT; commit3: ST
		//if err := newState.Database().TrieDB().Commit(root, true, nil); err != nil {
		if err := newState.Database().TrieDB().Commit2(root, blockNumber, true, nil); err != nil {
			return common.Hash{}, fmt.Errorf("failed to commit trie: %v", err)
		}
		*currentRoot = &trie.NewKey{Num: blockNumber, Depth: 0, NodeIndex: 0, Hash: root}

		return root, nil
	}

	return common.Hash{}, nil
}

func showStat2(ancientDb ethdb.Database) {
	// 获取 LevelDB 统计信息
	stats, err := ancientDb.Stat("leveldb.stats")
	if err != nil {
		log.Fatalf("Failed to get LevelDB stats: %v", err)
	}
	fmt.Println("LevelDB Stats:", stats)

	// 获取 LevelDB IO 统计信息
	iostats, err := ancientDb.Stat("leveldb.iostats")
	if err != nil {
		log.Fatalf("Failed to get LevelDB IO stats: %v", err)
	}
	fmt.Println("LevelDB IO Stats:", iostats)

}

func showRocksdbStat3(db ethdb.Database) {
	// Get the statistics for all column families
	stats, err := db.Stat("rocksdb.stats")
	if err != nil {
		log.Fatalf("Failed to get rocksdb.stats: %v", err)
	}
	fmt.Printf("rocksdb.stats:\n%s\n", stats)

}

func saveCurrentRoot(currentRoot *trie.NewKey, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(currentRoot)
}

func loadCurrentRoot(filename string) (*trie.NewKey, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var currentRoot trie.NewKey
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&currentRoot)
	if err != nil {
		return nil, err
	}

	return &currentRoot, nil
}

func calculateAverageTransactionTime() time.Duration {
	var total time.Duration
	for _, t := range transactionTimes {
		total += t
	}
	if len(transactionTimes) == 0 {
		return 0
	}
	return total / time.Duration(len(transactionTimes))
}

func saveDurationsToJSON(filename string) {
	// Convert durations to milliseconds
	var durationsInMilliseconds []float64
	for _, duration := range rawdb.Durations {
		durationsInMilliseconds = append(durationsInMilliseconds, float64(duration)/float64(time.Millisecond))
	}

	// Create the JSON file
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Encode the durations to JSON
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(durationsInMilliseconds); err != nil {
		log.Fatalf("Failed to encode JSON: %v", err)
	}
}
