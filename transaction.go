package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	hotk uint
)

const (
	max_key     = 5000
	num_routine = 100
	receiver    = 5
)

var sum uint

func prepare() {
	flag.UintVar(&hotk, "hotkey", 0, "enable hot key")

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

func key(i uint) string {
	if i >= max_key {
		panic("invalid key")
	}
	return fmt.Sprintf("key%d", i)
}

func hotkey(i uint) string {
	if i >= hotk {
		panic("invalid hot key")
	}
	return fmt.Sprintf("hot_key%d", i)
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
			var h uint
			for h = 0; h < hotk; h++ {
				pipe.Incr(ctx, hotkey(h))
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
		logger.Printf("check_sum failed %d != %d", sum2, sum)
	}
}

func test_txn() {
	prepare()
	logger.Printf("start benchmark %d goroutines * %d requests", num_routine, numReq)
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(num_routine)
	for i := 0; i < num_routine; i++ {
		go func() {
			ctx := context.Background()
			for r := 0; r < numReq; r++ {
				i := uint(rand.Uint32()) % max_key
				transfer(ctx, i)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	cost := time.Since(start)

	totalReq := numReq * num_routine
	tps := float64(totalReq) / cost.Seconds()
	logger.Printf("finished %d request cost %v: TPS=%2f", totalReq, cost, tps)

	check_sum()
}
