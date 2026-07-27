package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	mu   sync.Mutex
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

func (ck *Clerk) getOrMakeClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if c, ok := ck.rcks[gid]; ok {
		return c
	}
	c := shardgrp.MakeClerk(ck.clnt, servers)
	ck.rcks[gid] = c
	return c
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		shard := shardcfg.Key2Shard(key)
		config := ck.sck.Query()
		gid, servers, _ := config.GidServers(shard)
		clerk := ck.getOrMakeClerk(gid, servers)

		val, ver, err := clerk.Get(key)

		if err == rpc.ErrWrongGroup || err == rpc.ErrFrozen ||
			err == rpc.ErrWrongLeader || err == rpc.ErrMaybe {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return val, ver, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	resend := 0
	for {
		shard := shardcfg.Key2Shard(key)
		config := ck.sck.Query()
		gid, servers, _ := config.GidServers(shard)
		clerk := ck.getOrMakeClerk(gid, servers)

		// fmt.Printf("Put Version: %d\n", version)
		err := clerk.Put(key, value, version)

		switch err {
		case rpc.ErrWrongGroup, rpc.ErrFrozen, rpc.ErrWrongLeader:
			resend++
			time.Sleep(10 * time.Millisecond)
			continue
		case rpc.ErrVersion:
			if resend > 0 {
				return rpc.ErrMaybe
			}
			return err
		default:
			return err
		}
	}
}
