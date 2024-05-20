package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
)

var sha string

func eval_prepare() {
	ctx := context.Background()
	pipe := rdb.Pipeline()
	var i uint
	for i = 0; i < maxKey; i++ {
		_, err := pipe.HSet(ctx, "foo", i, i).Result()
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
	val, err := rdb.EvalSha(ctx, sha, []string{"foo", strconv.Itoa(i)}).Result()
	assert_ok(err)
	exp := fmt.Sprintf("BAR%d", i)
	if val != exp {
		panic(fmt.Sprintf("expected %s, but got %s", exp, val))
	}
}
