package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
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

func queryBadger(db *badger.DB, bloomSize uint, query eventsQuery) error {
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
					fmt.Println("last", block)
					break
				}
			}
		}

		return nil
	})

	duration := time.Now().Sub(start)
	fmt.Printf("found badger events %v %s\n", len(found), duration)

	return err
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

func ingest(ingestCmd *flag.FlagSet) {
	inputJSONFile := ingestCmd.String("input-json", "", "the json file containing the events")
	badgerDBFile := ingestCmd.String("badger-db", "", "the location of the badger db")
	ingestCmd.Parse(os.Args[2:])

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

func query(queryCmd *flag.FlagSet) {
	target := queryCmd.String("target", tetherAddress.String(), "the smart contract address which will be included in the filter")
	startBlock := queryCmd.Uint64("start", 16007143, "the starting block which will be included in the filter")
	endBlock := queryCmd.Uint64("end", 16007143+2000, "the ending block which will be included in the filter")
	badgerDBFile := queryCmd.String("badger-db", "", "the location of the badger db")
	queryCmd.Parse(os.Args[2:])

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
	if err := queryBadger(badgerDB, bloomSize, query); err != nil {
		log.Fatalf("could not query badger for events %v", err)
	}
}

func main() {
	ingestCmd := flag.NewFlagSet("ingest", flag.ExitOnError)
	queryCmd := flag.NewFlagSet("query", flag.ExitOnError)

	if len(os.Args) < 2 {
		log.Fatal("expected 'ingest' or 'query' subcommands")
	}

	switch os.Args[1] {
	case ingestCmd.Name():
		ingest(ingestCmd)
	case queryCmd.Name():
		query(queryCmd)
	default:
		log.Fatal("expected 'ingest' or 'query' subcommands")
	}
}
