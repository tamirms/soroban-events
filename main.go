package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stellar/go/support/log"
)

var rocketPoolAddress = common.HexToAddress("0xd33526068d116ce69f19a9ee46f0bd304f21a51f")
var tetherAddress = common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")

const defaultLimit = 10000

type eventsDB interface {
	ingest(inputJSON io.Reader) (time.Duration, error)
	query(query eventsQuery) (time.Duration, error)
	close() error
}

func newEventsDB(dbType, dbFilePath string) eventsDB {
	var e eventsDB
	var err error

	switch dbType {
	case "badger":
		e, err = newBadgerEventsDB(dbFilePath)
	case "sqlite":
		e, err = newSQLiteEventsDB(dbFilePath)
	case "postgres":
		e, err = newPostgresEventsDB(dbFilePath)
	case "memory":
		e, err = newMemoryEventsDB(dbFilePath)
	default:
		log.Fatalf("invalid database type: %v", dbType)
	}

	if err != nil {
		log.Fatalf("cannot create %v database: %v", dbType, err)
	}

	return e
}

type eventsQuery struct {
	target     common.Address
	startBlock uint64
	endBlock   uint64
	limit      int
}

func ingestCmd(ingestFlags *flag.FlagSet) {
	inputJSONFile := ingestFlags.String("input-json", "", "the json file containing the events")
	dbFilePath := ingestFlags.String("db-file", "", "file path of the database")
	dbType := ingestFlags.String("db", "", "the type of database to use: badger, postgres, sqlite, or memory")
	ingestFlags.Parse(os.Args[2:])

	file, err := os.Open(*inputJSONFile)
	if err != nil {
		log.Fatalf("cannot open events json: %v", err)
	}
	defer file.Close()

	edb := newEventsDB(*dbType, *dbFilePath)
	defer edb.close()

	if duration, err := edb.ingest(file); err != nil {
		log.Fatalf("cannot ingest events: %v", err)
	} else {
		fmt.Printf("ingest duration %v\n", duration)
	}
}

func queryCmd(queryFlags *flag.FlagSet) {
	target := queryFlags.String("target", tetherAddress.String(), "the smart contract address which will be included in the filter")
	startBlock := queryFlags.Uint64("start", 16007143, "the starting block which will be included in the filter")
	endBlock := queryFlags.Uint64("end", 16007143+2000, "the ending block which will be included in the filter")
	dbFilePath := queryFlags.String("db-file", "", "file path of the database")
	dbType := queryFlags.String("db", "", "the type of database to use: badger, postgres, sqlite, or memory")
	queryFlags.Parse(os.Args[2:])

	q := eventsQuery{
		target:     common.HexToAddress(*target),
		startBlock: *startBlock,
		endBlock:   *endBlock,
		limit:      defaultLimit,
	}
	edb := newEventsDB(*dbType, *dbFilePath)
	defer edb.close()

	if duration, err := edb.query(q); err != nil {
		log.Fatalf("cannot query events: %v", err)
	} else {
		fmt.Printf("query duration %v\n", duration)
	}
}

func benchmarkCmd(benchmarkFlags *flag.FlagSet) {
	dbFilePath := benchmarkFlags.String("db-file", "", "file path of the database")
	dbType := benchmarkFlags.String("db", "", "the type of database to use: badger, postgres, sqlite, or memory")
	requestsFlag := benchmarkFlags.Int("requests", 1000, "total number of requests")
	threadsFlag := benchmarkFlags.Int("threads", runtime.NumCPU(), "number of goroutine workers")
	target := benchmarkFlags.String("target", tetherAddress.String(), "the smart contract address which will be included in the filter")

	benchmarkFlags.Parse(os.Args[2:])

	edb := newEventsDB(*dbType, *dbFilePath)
	defer edb.close()

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
			for q := range queue {
				if duration, err := edb.query(q); err != nil {
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
