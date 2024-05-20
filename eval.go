package main

import (
	"context"
	"time"
)

func test_eval() {
	ctx := context.Background()
	_, err := rdb.HSet(ctx, "foo", "1", 1).Result()
	assert_ok(err)
	_, err = rdb.Set(ctx, "bar1", "bar-1", 0).Result()
	assert_ok(err)
	lua := `
	local n = redis.call('HGET', KEYS[1], KEYS[2])
	return redis.call('GET', 'bar' .. string.format("%.f", n))
	`
	sha, err := rdb.ScriptLoad(ctx, lua).Result()
	assert_ok(err)
	start := time.Now()
	for i := 0; i < numReq; i++ {
		_, err = rdb.EvalSha(ctx, sha, []string{"foo", "1"}).Result()
		assert_ok(err)
	}
	elapsed := time.Since(start)
	logger.Println("cost", elapsed)
}
