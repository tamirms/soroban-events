package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	badger "github.com/dgraph-io/badger/v3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/stellar/go/support/log"
)

var rocketPoolAddress = common.HexToAddress("0xd33526068d116ce69f19a9ee46f0bd304f21a51f")
var tetherAddress = common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")

const bloomSize = 2500
const defaultLimit = 10000

func parseBlockFromBloom(key string) uint64 {
	block, err := strconv.ParseUint(key[len("b:"):], 10, 32)
	if err != nil {
		log.Fatalf("could not extract block %v", err)
	}
	return block
}

func bloomKey(block uint64) string {
	return fmt.Sprintf("b:%010d", block)
}

func eventKey(block uint64, index uint) string {
	id := (block << 32) | uint64(index)
	return fmt.Sprintf("e:%020d", id)
}

type eventsQuery struct {
	target     common.Address
	startBlock uint64
	endBlock   uint64
	limit      int
}

func queryBadger(db *badger.DB, bloomSize uint, query eventsQuery) (time.Duration, error) {
	var found []types.Log
	start := time.Now()
	err := db.View(func(txn *badger.Txn) error {
		bloomIt := txn.NewIterator(badger.DefaultIteratorOptions)
		defer bloomIt.Close()
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		startBloomKey := bloomKey(query.startBlock)
		endBloomKey := bloomKey(query.endBlock)
		filter := newBlockBloomFilter(bloomSize)
		for bloomIt.Seek([]byte(startBloomKey)); bloomIt.ValidForPrefix([]byte("b:")); bloomIt.Next() {
			item := bloomIt.Item()
			k := item.Key()
			if string(k[:len(endBloomKey)]) > endBloomKey {
				break
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err = filter.GobDecode(val); err != nil {
				return err
			}
			if filter.Test(query.target.Bytes()) {
				block := parseBlockFromBloom(string(k))
				found, err = findEvents(it, eventsQuery{
					target:     query.target,
					startBlock: block,
					endBlock:   block,
					limit:      query.limit,
				}, found)
				if err != nil {
					return err
				}
				if len(found) >= query.limit {
					break
				}
			}
		}

		return nil
	})

	duration := time.Now().Sub(start)

	return duration, err
}

func findEvents(it *badger.Iterator, query eventsQuery, found []types.Log) ([]types.Log, error) {
	startKey := eventKey(query.startBlock, 0)
	endKey := eventKey(query.endBlock+1, 0)
	for it.Seek([]byte(startKey)); it.ValidForPrefix([]byte("e:")); it.Next() {
		item := it.Item()
		k := item.Key()
		if string(k[:len(endKey)]) >= endKey {
			break
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		var event types.Log
		if err := rlp.DecodeBytes(val, &event); err != nil {
			return nil, err
		}
		if event.Address == query.target {
			found = append(found, event)
		}
		if len(found) >= query.limit {
			break
		}
	}
	return found, nil
}

func ingestBadger(db *badger.DB, file io.Reader, bloomSize uint) error {
	start := time.Now()

	batch := db.NewWriteBatch()
	decoder := json.NewDecoder(file)
	var curBlock uint64
	filter := newBlockBloomFilter(bloomSize)

	curItemsPerBlock, curEventsPerBlock, maxItemsPerBlock, maxEventsPerBlock := 0, 0, 0, 0
	numEvents := 0

	for {
		var event types.Log
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				if err := writeBloomFilter(curBlock, filter, batch); err != nil {
					return err
				}
				break
			}
			return err
		}
		numEvents++
		if event.BlockNumber > curBlock {
			if err := writeBloomFilter(curBlock, filter, batch); err != nil {
				return err
			}
			filter = newBlockBloomFilter(bloomSize)
			curBlock = event.BlockNumber

			if curItemsPerBlock > maxItemsPerBlock {
				maxItemsPerBlock = curItemsPerBlock
			}
			curItemsPerBlock = 0

			if curEventsPerBlock > maxEventsPerBlock {
				maxEventsPerBlock = curEventsPerBlock
			}
			curEventsPerBlock = 0
		}
		key := eventKey(event.BlockNumber, event.Index)
		var buf bytes.Buffer
		if err := event.EncodeRLP(&buf); err != nil {
			return err
		}

		if err := batch.Set([]byte(key), buf.Bytes()); err != nil {
			return err
		}
		filter.Add(event.Address.Bytes())
		curItemsPerBlock++
		curEventsPerBlock++
		for i, topic := range event.Topics {
			filter.Add(append(topic.Bytes(), byte(i)))
			curItemsPerBlock++
		}
	}
	if curItemsPerBlock > maxItemsPerBlock {
		maxItemsPerBlock = curItemsPerBlock
	}
	if curEventsPerBlock > maxEventsPerBlock {
		maxEventsPerBlock = curEventsPerBlock
	}
	err := batch.Flush()
	duration := time.Since(start)
	fmt.Printf("maxItemsPerBlock %v maxEventsPerBlock %v numEvents %v duration %v\n", maxItemsPerBlock, maxEventsPerBlock, numEvents, duration)
	return err
}

func newBlockBloomFilter(size uint) *bloom.BloomFilter {
	return bloom.NewWithEstimates(size, 0.01)
}

func writeBloomFilter(curBlock uint64, filter *bloom.BloomFilter, batch *badger.WriteBatch) error {
	if curBlock == 0 {
		return nil
	}
	payload, err := filter.GobEncode()
	if err != nil {
		return err
	}
	if err := batch.Set([]byte(bloomKey(curBlock)), payload); err != nil {
		return err
	}
	return nil
}

func ingestCmd(ingestFlags *flag.FlagSet) {
	inputJSONFile := ingestFlags.String("input-json", "", "the json file containing the events")
	badgerDBFile := ingestFlags.String("badger-db", "", "the location of the badger db")
	ingestFlags.Parse(os.Args[2:])

	file, err := os.Open(*inputJSONFile)
	if err != nil {
		log.Fatalf("cannot open events json: %v", err)
	}
	defer file.Close()

	opts := badger.DefaultOptions(*badgerDBFile)
	badgerDB, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("cannot open badger db: %v", err)
	}
	defer badgerDB.Close()

	if err = ingestBadger(badgerDB, file, bloomSize); err != nil {
		log.Fatalf("cannot ingest into badger: %v", err)
	}
}

func queryCmd(queryFlags *flag.FlagSet) {
	target := queryFlags.String("target", tetherAddress.String(), "the smart contract address which will be included in the filter")
	startBlock := queryFlags.Uint64("start", 16009543, "the starting block which will be included in the filter")
	endBlock := queryFlags.Uint64("end", 16011543, "the ending block which will be included in the filter")
	badgerDBFile := queryFlags.String("badger-db", "", "the location of the badger db")
	queryFlags.Parse(os.Args[2:])

	opts := badger.DefaultOptions(*badgerDBFile)
	badgerDB, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("cannot open badger db: %v", err)
	}
	defer badgerDB.Close()

	query := eventsQuery{
		target:     common.HexToAddress(*target),
		startBlock: *startBlock,
		endBlock:   *endBlock,
		limit:      defaultLimit,
	}
	if duration, err := queryBadger(badgerDB, bloomSize, query); err != nil {
		log.Fatalf("could not query badger for events %v", err)
	} else {
		fmt.Printf("completed query in %v\n", duration)
	}
}

func benchmarkCmd(benchmarkFlags *flag.FlagSet) {
	badgerDBFile := benchmarkFlags.String("badger-db", "", "the location of the badger db")
	requestsFlag := benchmarkFlags.Int("requests", 100, "total number of requests")
	threadsFlag := benchmarkFlags.Int("threads", runtime.NumCPU(), "number of goroutine workers")
	target := benchmarkFlags.String("target", tetherAddress.String(), "the smart contract address which will be included in the filter")

	benchmarkFlags.Parse(os.Args[2:])

	opts := badger.DefaultOptions(*badgerDBFile).WithBlockCacheSize(512 << 20)
	badgerDB, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("cannot open badger db: %v", err)
	}
	defer badgerDB.Close()

	totalRequests := *requestsFlag
	targetAddress := common.HexToAddress(*target)

	queue := make(chan eventsQuery, totalRequests)
	results := make(chan time.Duration, totalRequests)
	threads := *threadsFlag
	fmt.Printf("using %v cpus\n", threads)

	var wg sync.WaitGroup

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for query := range queue {
				if duration, err := queryBadger(badgerDB, bloomSize, query); err != nil {
					log.Fatalf("could not query badger for events %v", err)
				} else {
					results <- duration
				}
			}
		}()
	}

	lowerBound := int64(16007143)
	upperBound := int64(16024423 - 2000)
	diff := upperBound + 1 - lowerBound

	startTime := time.Now()

	for i := 0; i < totalRequests; i++ {
		start := uint64(lowerBound + rand.Int63n(diff))
		queue <- eventsQuery{
			target:     targetAddress,
			startBlock: start,
			endBlock:   start + 2000,
			limit:      defaultLimit,
		}
	}
	close(queue)
	wg.Wait()
	close(results)

	totalDuration := time.Since(startTime)

	fmt.Printf("total duration %v\n", totalDuration)

	var samples []int
	for duration := range results {
		samples = append(samples, int(duration.Milliseconds()))
	}
	sort.Sort(sort.IntSlice(samples))
	fmt.Println(samples)
}

func main() {
	ingestFlags := flag.NewFlagSet("ingest", flag.ExitOnError)
	queryFlags := flag.NewFlagSet("query", flag.ExitOnError)
	benchmarkFlags := flag.NewFlagSet("benchmark", flag.ExitOnError)

	if len(os.Args) < 2 {
		log.Fatal("expected 'ingest' or 'query' subcommands")
	}

	switch os.Args[1] {
	case ingestFlags.Name():
		ingestCmd(ingestFlags)
	case queryFlags.Name():
		queryCmd(queryFlags)
	case benchmarkFlags.Name():
		benchmarkCmd(benchmarkFlags)
	default:
		log.Fatal("expected 'ingest' or 'query' subcommands")
	}
}
