package shardgrp

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int
	mu      sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers, leader: 0, mu: sync.Mutex{}}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	ck.mu.Lock()
	index := ck.leader
	ck.mu.Unlock()

	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	ok := ck.Clnt.Call(ck.servers[index], "KVServer.Get", &args, &reply)
	if !ok || reply.Err == rpc.ErrWrongLeader {
		ck.mu.Lock()
		ck.leader = (index + 1) % len(ck.servers)
		ck.mu.Unlock()
		return "", 0, rpc.ErrWrongLeader
	}

	ck.mu.Lock()
	ck.leader = index
	ck.mu.Unlock()
	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	ck.mu.Lock()
	index := ck.leader
	ck.mu.Unlock()

	ok := ck.Clnt.Call(ck.servers[index], "KVServer.Put", &args, &reply)
	if !ok {
		ck.mu.Lock()
		ck.leader = (index + 1) % len(ck.servers)
		ck.mu.Unlock()
		return rpc.ErrWrongLeader
	}
	if reply.Err == rpc.ErrWrongLeader {
		ck.mu.Lock()
		ck.leader = (index + 1) % len(ck.servers)
		ck.mu.Unlock()
		return rpc.ErrWrongLeader
	}

	ck.mu.Lock()
	ck.leader = index
	ck.mu.Unlock()
	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum, stale func() bool) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}

	ck.mu.Lock()
	index := ck.leader
	ck.mu.Unlock()

	attempt := 0
	for {
		if stale != nil && stale() {
			return nil, rpc.ErrStale
		}
		attempt++
		// fmt.Fprintf(os.Stderr, "[Clerk.FreezeShard] attempt=%d shard=%d num=%d server=%s (index=%d/%d)\n",
		// 	attempt, s, num, ck.servers[index], index, len(ck.servers))

		ok := ck.Clnt.Call(ck.servers[index], "KVServer.FreezeShard", &args, &reply)

		// fmt.Fprintf(os.Stderr, "[Clerk.FreezeShard] attempt=%d shard=%d num=%d server=%s ok=%v err=%v\n",
		// 	attempt, s, num, ck.servers[index], ok, reply.Err)

		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrStale) {
			break
		}
		index = (index + 1) % len(ck.servers)
		// fmt.Printf("Freeze trying server index %d %s\n", index, reply.Err)
		time.Sleep(20 * time.Millisecond)
		reply = shardrpc.FreezeShardReply{}
	}

	ck.mu.Lock()
	ck.leader = index
	ck.mu.Unlock()

	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum, stale func() bool) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{}

	ck.mu.Lock()
	index := ck.leader
	ck.mu.Unlock()

	attempt := 0
	for {
		if stale != nil && stale() {
			return rpc.ErrStale
		}
		attempt++
		// fmt.Fprintf(os.Stderr, "[Clerk.InstallShard] attempt=%d shard=%d num=%d server=%s (index=%d/%d)\n",
		// 	attempt, s, num, ck.servers[index], index, len(ck.servers))

		ok := ck.Clnt.Call(ck.servers[index], "KVServer.InstallShard", &args, &reply)

		// fmt.Fprintf(os.Stderr, "[Clerk.InstallShard] attempt=%d shard=%d num=%d server=%s ok=%v err=%v\n",
		// 	attempt, s, num, ck.servers[index], ok, reply.Err)
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrStale) {
			break
		}

		index = (index + 1) % len(ck.servers)
		// fmt.Printf("Install trying server index %d %s\n", index, reply.Err)
		time.Sleep(20 * time.Millisecond)
		reply = shardrpc.InstallShardReply{}
	}
	ck.mu.Lock()
	ck.leader = index
	ck.mu.Unlock()

	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum, stale func() bool) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}

	ck.mu.Lock()
	index := ck.leader
	ck.mu.Unlock()

	attempt := 0
	for {
		if stale != nil && stale() {
			return rpc.ErrStale
		}
		attempt++
		// fmt.Fprintf(os.Stderr, "[Clerk.DeleteShard] attempt=%d shard=%d num=%d server=%s (index=%d/%d)\n",
		// 	attempt, s, num, ck.servers[index], index, len(ck.servers))

		ok := ck.Clnt.Call(ck.servers[index], "KVServer.DeleteShard", &args, &reply)

		// fmt.Fprintf(os.Stderr, "[Clerk.DeleteShard] attempt=%d shard=%d num=%d server=%s ok=%v err=%v\n",
		// 	attempt, s, num, ck.servers[index], ok, reply.Err)

		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrStale) {
			break
		}

		index = (index + 1) % len(ck.servers)
		// fmt.Printf("Delete trying server index %d %s\n", index, reply.Err)
		time.Sleep(20 * time.Millisecond)
		reply = shardrpc.DeleteShardReply{}
	}

	ck.mu.Lock()
	ck.leader = index
	ck.mu.Unlock()

	return reply.Err
}
