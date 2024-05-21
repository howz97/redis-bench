package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
)

var (
	shards uint64
)

var sha string

func eval_key(i uint64) string {
	return fmt.Sprintf("foo%d", i%shards)
}

func packEntry(typ uint8, inode uint64) []byte {
	wb := NewBuffer(9)
	wb.Put8(typ)
	wb.Put64(inode)
	return wb.Bytes()
}

func eval_prepare() {
	ctx := context.Background()
	pipe := rdb.Pipeline()
	var i uint64
	for i = 0; i < maxKey; i++ {
		key := eval_key(i)
		_, err := pipe.HSet(ctx, key, i, packEntry(1, i)).Result()
		assert_ok(err)
		_, err = pipe.Set(ctx, fmt.Sprintf("inode%d", i), fmt.Sprintf("BAR%d", i), 0).Result()
		assert_ok(err)
	}
	pipe.Exec(ctx)
	lua, err := os.ReadFile(luaFile)
	assert_ok(err)
	sha, err = rdb.ScriptLoad(ctx, string(lua)).Result()
	assert_ok(err)
}

func eval_test(ctx context.Context) {
	i := rand.Intn(int(maxKey))
	key := eval_key(uint64(i))
	field := strconv.Itoa(i)
	val, err := rdb.EvalSha(ctx, sha, []string{key, field}).Result()
	if err != nil {
		panic(fmt.Sprintf("EvalSha %s/%s: %v", key, field, err))
	}
	exp := fmt.Sprintf("BAR%d", i)
	if val != exp {
		panic(fmt.Sprintf("expected %s, but got %s", exp, val))
	}
}
