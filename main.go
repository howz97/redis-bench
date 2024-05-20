package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var server string
var numReq int
var test string
var numRoutine int
var maxKey uint

var rdb *redis.Client
var logger *log.Logger

func init() {
	flag.StringVar(&server, "server", "127.0.0.1:6379", "redis server address")
	flag.IntVar(&numReq, "requests", 1000, "how many requests to execute")
	flag.StringVar(&test, "test", "eval", "test redis commands")
	flag.IntVar(&numRoutine, "routine", 16, "number of goroutine")
	flag.UintVar(&maxKey, "maxkey", 0, "max key range")
	flag.Parse()

	logger = log.Default()
	rdb = redis.NewClient(&redis.Options{
		Addr: server,
	})
}

func assert_ok(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if maxKey == 0 {
		maxKey = uint(numRoutine)
	}
	var f func(ctx context.Context)
	switch test {
	case "eval":
		eval_prepare()
		f = eval_test
	case "txn":
		txn_prepare()
		f = txn_test
	}

	logger.Printf("start benchmark %d goroutines * %d requests", numRoutine, numReq)
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(numRoutine)
	for i := 0; i < numRoutine; i++ {
		go func() {
			ctx := context.Background()
			for r := 0; r < numReq; r++ {
				f(ctx)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	cost := time.Since(start)
	totalReq := numReq * numRoutine
	qps := float64(totalReq) / cost.Seconds()
	logger.Printf("finished %d request cost %v: QPS=%2f", totalReq, cost, qps)

	switch test {
	case "txn":
		txn_post()
	}
}
