package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int

	mu sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, mu: sync.Mutex{}, leader: 0}
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

	for {
		ok := ck.clnt.Call(ck.servers[index], "KVServer.Get", &args, &reply)

		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey) {
			break
		}

		index = (index + 1) % len(ck.servers)

		time.Sleep(20 * time.Millisecond)
		reply = rpc.GetReply{}
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

	resend := 0
	for {
		ok := ck.clnt.Call(ck.servers[index], "KVServer.Put", &args, &reply)

		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrVersion || reply.Err == rpc.ErrNoKey) {
			if resend > 0 && reply.Err == rpc.ErrVersion {
				reply.Err = rpc.ErrMaybe
			}
			break
		}

		index = (index + 1) % len(ck.servers)
		resend++
		time.Sleep(20 * time.Millisecond)
		reply = rpc.PutReply{}
	}

	ck.mu.Lock()
	ck.leader = index
	ck.mu.Unlock()

	return reply.Err
}
