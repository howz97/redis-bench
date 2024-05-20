package main

import (
	"flag"
	"log"

	"github.com/redis/go-redis/v9"
)

var server string
var numReq int
var test string

var rdb *redis.Client
var logger *log.Logger

func init() {
	flag.StringVar(&server, "server", "127.0.0.1:6379", "redis server address")
	flag.IntVar(&numReq, "requests", 1000, "how many requests to execute")
	flag.StringVar(&test, "test", "eval", "test redis commands")
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
	switch test {
	case "eval":
		test_eval()
	case "txn":
		test_txn()
	}
}
