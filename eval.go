package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
)

var (
	shards uint
)

var sha string

func eval_key(i uint) string {
	return fmt.Sprintf("foo%d", i%shards)
}

func eval_prepare() {
	ctx := context.Background()
	pipe := rdb.Pipeline()
	var i uint
	for i = 0; i < maxKey; i++ {
		key := eval_key(i)
		_, err := pipe.HSet(ctx, key, i, i).Result()
		assert_ok(err)
		_, err = pipe.Set(ctx, fmt.Sprintf("bar%d", i), fmt.Sprintf("BAR%d", i), 0).Result()
		assert_ok(err)
	}
	pipe.Exec(ctx)
	lua := `
	local n = redis.call('HGET', KEYS[1], KEYS[2])
	return redis.call('GET', 'bar' .. string.format("%.f", n))
	`
	var err error
	sha, err = rdb.ScriptLoad(ctx, lua).Result()
	assert_ok(err)
}

func eval_test(ctx context.Context) {
	i := rand.Intn(int(maxKey))
	key := eval_key(uint(i))
	val, err := rdb.EvalSha(ctx, sha, []string{key, strconv.Itoa(i)}).Result()
	assert_ok(err)
	exp := fmt.Sprintf("BAR%d", i)
	if val != exp {
		panic(fmt.Sprintf("expected %s, but got %s", exp, val))
	}
}
