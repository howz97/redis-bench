package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var (
	hotk uint
)

const receiver = 5

var sum uint64

func txn_prepare() {
	ctx := context.Background()
	logger.Println("initializing key values")
	pipe := rdb.Pipeline()
	var i uint64
	for i = 0; i < maxKey; i++ {
		v := 1000 + i
		err := pipe.Set(ctx, key(i), v, 0).Err()
		assert_ok(err)
		sum += v
	}
	pipe.Exec(ctx)
}

func key(i uint64) string {
	if i >= maxKey {
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

func transfer(ctx context.Context, i uint64) {
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
				rcv := (2*i + uint64(r)) % maxKey
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

func txn_post() {
	ctx := context.Background()
	var sum2 uint64
	var i uint64
	for i = 0; i < maxKey; i++ {
		val, err := rdb.Get(ctx, key(i)).Result()
		assert_ok(err)
		balance, err := strconv.Atoi(val)
		assert_ok(err)
		sum2 += uint64(balance)
	}
	if sum2 != sum {
		logger.Printf("check_sum failed %d != %d", sum2, sum)
	}
}

func txn_test(ctx context.Context) {
	i := rand.Uint64() % maxKey
	transfer(ctx, i)
}
