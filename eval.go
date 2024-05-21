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
		attr := Attr{}
		attr.Length = i
		_, err = pipe.Set(ctx, fmt.Sprintf("inode%d", i), marshal(&attr), 0).Result()
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
	buf, err := rdb.EvalSha(ctx, sha, []string{key, field}).Result()
	if err != nil {
		panic(fmt.Sprintf("EvalSha %s/%s: %v", key, field, err))
	}
	attr := Attr{}
	parseAttr([]byte(buf.(string)), &attr)
	if attr.Length != uint64(i) {
		panic(fmt.Sprintf("expected %v, but got %v", i, attr.Length))
	}
}

// Attr represents attributes of a node.
type Attr struct {
	Flags     uint8  // flags
	Typ       uint8  // type of a node
	Mode      uint16 // permission mode
	Uid       uint32 // owner id
	Gid       uint32 // group id of owner
	Rdev      uint32 // device number
	Atime     int64  // last access time
	Mtime     int64  // last modified time
	Ctime     int64  // last change time for meta
	Atimensec uint32 // nanosecond part of atime
	Mtimensec uint32 // nanosecond part of mtime
	Ctimensec uint32 // nanosecond part of ctime
	Nlink     uint32 // number of links (sub-directories or hardlinks)
	Length    uint64 // length of regular file

	Parent    uint64 // inode of parent; 0 means tracked by parentKey (for hardlinks)
	Full      bool   // the attributes are completed or not
	KeepCache bool   // whether to keep the cached page or not

	AccessACL  uint32 // access ACL id (identical ACL rules share the same access ACL ID.)
	DefaultACL uint32 // default ACL id (default ACL and the access ACL share the same cache and store)
}

func parseAttr(buf []byte, attr *Attr) {
	if attr == nil || len(buf) == 0 {
		return
	}
	rb := FromBuffer(buf)
	attr.Flags = rb.Get8()
	attr.Mode = rb.Get16()
	attr.Typ = uint8(attr.Mode >> 12)
	attr.Mode &= 0xfff
	attr.Uid = rb.Get32()
	attr.Gid = rb.Get32()
	attr.Atime = int64(rb.Get64())
	attr.Atimensec = rb.Get32()
	attr.Mtime = int64(rb.Get64())
	attr.Mtimensec = rb.Get32()
	attr.Ctime = int64(rb.Get64())
	attr.Ctimensec = rb.Get32()
	attr.Nlink = rb.Get32()
	attr.Length = rb.Get64()
	attr.Rdev = rb.Get32()
	if rb.Left() >= 8 {
		attr.Parent = rb.Get64()
	}
	attr.Full = true
	if rb.Left() >= 8 {
		attr.AccessACL = rb.Get32()
		attr.DefaultACL = rb.Get32()
	}
}

func marshal(attr *Attr) []byte {
	size := uint32(36 + 24 + 4 + 8)
	if attr.AccessACL|attr.DefaultACL != 0 {
		size += 8
	}
	w := NewBuffer(size)
	w.Put8(attr.Flags)
	w.Put16((uint16(attr.Typ) << 12) | (attr.Mode & 0xfff))
	w.Put32(attr.Uid)
	w.Put32(attr.Gid)
	w.Put64(uint64(attr.Atime))
	w.Put32(attr.Atimensec)
	w.Put64(uint64(attr.Mtime))
	w.Put32(attr.Mtimensec)
	w.Put64(uint64(attr.Ctime))
	w.Put32(attr.Ctimensec)
	w.Put32(attr.Nlink)
	w.Put64(attr.Length)
	w.Put32(attr.Rdev)
	w.Put64(uint64(attr.Parent))
	if attr.AccessACL+attr.DefaultACL > 0 {
		w.Put32(attr.AccessACL)
		w.Put32(attr.DefaultACL)
	}
	return w.Bytes()
}
