package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	max_key     = 5000
	num_routine = 100
	receiver    = 5
)

var (
	num_req int
	address string
	sum     uint
)
var rdb *redis.Client
var logger *log.Logger

func init() {
	flag.StringVar(&address, "server", "127.0.0.1:6379", "redis server address")
	flag.IntVar(&num_req, "requests", 1000, "how many transaction each goroutine execute")
	flag.Parse()

	logger = log.Default()

	rdb = redis.NewClient(&redis.Options{
		Addr: address,
	})

	ctx := context.Background()
	logger.Println("initializing key values")
	pipe := rdb.Pipeline()
	var i uint
	for i = 0; i < max_key; i++ {
		v := 1000 + i
		err := pipe.Set(ctx, key(i), v, 0).Err()
		assert_ok(err)
		sum += v
	}
	pipe.Exec(ctx)
}

func main() {
	logger.Printf("start benchmark %d goroutines * %d requests", num_routine, num_req)
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(num_routine)
	for i := 0; i < num_routine; i++ {
		go func() {
			ctx := context.Background()
			for r := 0; r < num_req; r++ {
				i := uint(rand.Uint32()) % max_key
				transfer(ctx, i)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	cost := time.Since(start)

	totalReq := num_req * num_routine
	qps := float64(totalReq) / cost.Seconds()
	logger.Printf("finished %d request cost %v: QPS=%2f", totalReq, cost, qps)

	// check_sum()
}

func key(i uint) string {
	return fmt.Sprintf("key%d", i)
}

func assert_ok(err error) {
	if err != nil {
		panic(err)
	}
}

func transfer(ctx context.Context, i uint) {
	key_src := key(i)
	err := rdb.Watch(ctx, func(tx *redis.Tx) error {
		val, err := tx.Get(ctx, key_src).Result()
		assert_ok(err)
		balance, err := strconv.Atoi(val)
		assert_ok(err)
		if balance < receiver {
			return nil
		}
		tx.Set(ctx, key_src, balance-receiver, 0)

		_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for r := 0; r < receiver; r++ {
				rcv := (2*i + uint(r)) % max_key
				pipe.Incr(ctx, key(rcv))
			}
			return nil
		})
		return err
	}, key_src)
	assert_ok(err)
}

func check_sum() {
	ctx := context.Background()
	var sum2 uint
	var i uint
	for i = 0; i < max_key; i++ {
		val, err := rdb.Get(ctx, key(i)).Result()
		assert_ok(err)
		balance, err := strconv.Atoi(val)
		assert_ok(err)
		sum2 += uint(balance)
	}
	if sum2 != sum {
		logger.Fatalf("check_sum failed %d != %d", sum2, sum)
	}
}
