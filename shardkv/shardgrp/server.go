package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type KVServer struct {
	me  int
	gid tester.Tgid
	rsm *rsm.RSM

	highestConfig map[shardcfg.Tshid]shardcfg.Tnum
	frozen        [shardcfg.NShards]bool
	owned         [shardcfg.NShards]bool
	valueStore    [shardcfg.NShards]map[string]rpc.Record

	mu sync.Mutex
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		shard := shardcfg.Key2Shard(op.Key)

		if kv.frozen[shard] {
			reply.Err = rpc.ErrFrozen
			return reply
		}

		if !kv.owned[shard] {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		if record, ok := kv.valueStore[shard][op.Key]; ok {
			reply.Value = record.Value
			reply.Version = record.Version
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply

	case rpc.PutArgs:
		reply := rpc.PutReply{}
		shard := shardcfg.Key2Shard(op.Key)

		if kv.frozen[shard] {
			reply.Err = rpc.ErrFrozen
			return reply
		}

		if !kv.owned[shard] {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		if record, ok := kv.valueStore[shard][op.Key]; ok {
			if op.Version == record.Version {
				kv.valueStore[shard][op.Key] = rpc.Record{Version: record.Version + 1, Value: op.Value}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
			// fmt.Printf("[DoOp %d] Put key=%s found=%v curVer=%v reqVer=%v -> %v\n", kv.me, op.Key, ok, record.Version, op.Version, reply.Err)
		} else {
			if op.Version == 0 {
				kv.valueStore[shard][op.Key] = rpc.Record{Version: 1, Value: op.Value}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
			// fmt.Printf("[DoOp %d] Put key=%s found=%v curVer=%v reqVer=%v -> %v\n", kv.me, op.Key, ok, record.Version, op.Version, reply.Err)
		}
		return reply

	case shardrpc.FreezeShardArgs:
		// fmt.Printf("[DoOp gid=%d] FreezeShard shard=%d num=%d owned=%v\n", kv.gid, op.Shard, op.Num, kv.owned[op.Shard])
		reply := shardrpc.FreezeShardReply{}

		if op.Num < kv.highestConfig[op.Shard] {
			reply.Err = rpc.ErrStale
			return reply
		}

		if !kv.owned[op.Shard] {
			if op.Num == kv.highestConfig[op.Shard] {
				reply.Err = rpc.OK
				reply.Num = kv.highestConfig[op.Shard]
				reply.State = nil
				return reply
			}
			reply.Err = rpc.ErrStale
			reply.Num = kv.highestConfig[op.Shard]
			return reply
		}

		if op.Num > kv.highestConfig[op.Shard] {
			kv.highestConfig[op.Shard] = op.Num
		}

		if ok := kv.frozen[op.Shard]; !ok {
			kv.frozen[op.Shard] = true
		}
		reply.Err = rpc.OK
		reply.Num = kv.highestConfig[op.Shard]
		reply.State = kv.SnapshotShard(op.Shard)
		return reply

	case shardrpc.InstallShardArgs:
		// fmt.Printf("[DoOp gid=%d] InstallShard shard=%d num=%d owned=%v\n", kv.gid, op.Shard, op.Num, kv.owned[op.Shard])
		reply := shardrpc.InstallShardReply{}

		if op.Num < kv.highestConfig[op.Shard] {
			reply.Err = rpc.ErrStale
			return reply
		}

		if op.Num == kv.highestConfig[op.Shard] && kv.owned[op.Shard] {
			reply.Err = rpc.OK
			return reply
		}

		kv.highestConfig[op.Shard] = op.Num
		kv.RestoreShard(op.Shard, op.State)
		kv.owned[op.Shard] = true
		kv.frozen[op.Shard] = false
		reply.Err = rpc.OK
		return reply

	case shardrpc.DeleteShardArgs:
		// fmt.Printf("[DoOp gid=%d] DeleteShard shard=%d num=%d owned=%v\n", kv.gid, op.Shard, op.Num, kv.owned[op.Shard])
		reply := shardrpc.DeleteShardReply{}

		if op.Num < kv.highestConfig[op.Shard] {
			reply.Err = rpc.ErrStale
			return reply
		}

		if op.Num == kv.highestConfig[op.Shard] && !kv.owned[op.Shard] {
			reply.Err = rpc.OK
			return reply
		}

		if op.Num > kv.highestConfig[op.Shard] {
			kv.highestConfig[op.Shard] = op.Num
		}

		kv.owned[op.Shard] = false
		kv.valueStore[op.Shard] = make(map[string]rpc.Record)
		reply.Err = rpc.OK
		return reply
	}

	return nil
}

func (kv *KVServer) SnapshotShard(shard shardcfg.Tshid) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.valueStore[shard])
	return w.Bytes()
}

func (kv *KVServer) RestoreShard(shard shardcfg.Tshid, data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&kv.valueStore[shard])
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// Assumes caller has lock
	e.Encode(kv.valueStore)
	e.Encode(kv.frozen)
	e.Encode(kv.owned)
	e.Encode(kv.highestConfig)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	d.Decode(&kv.valueStore)
	d.Decode(&kv.frozen)
	d.Decode(&kv.owned)
	d.Decode(&kv.highestConfig)
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, req := kv.rsm.Submit(*args)
	res, ok := req.(rpc.GetReply)
	reply.Err = err

	if ok {
		reply.Err = res.Err
		reply.Value = res.Value
		reply.Version = res.Version
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, req := kv.rsm.Submit(*args)
	res, ok := req.(rpc.PutReply)
	reply.Err = err

	if ok {
		reply.Err = res.Err
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, req := kv.rsm.Submit(*args)
	res, ok := req.(shardrpc.FreezeShardReply)
	reply.Err = err

	if ok {
		reply.Err = res.Err
		reply.State = res.State
		reply.Num = res.Num
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, req := kv.rsm.Submit(*args)
	res, ok := req.(shardrpc.InstallShardReply)
	reply.Err = err

	if ok {
		reply.Err = res.Err
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, req := kv.rsm.Submit(*args)
	res, ok := req.(shardrpc.DeleteShardReply)
	reply.Err = err

	if ok {
		reply.Err = res.Err
	}
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.mu = sync.Mutex{}
	kv.highestConfig = make(map[shardcfg.Tshid]shardcfg.Tnum)

	for i := 0; i < shardcfg.NShards; i++ {
		kv.valueStore[i] = make(map[string]rpc.Record)
		kv.owned[i] = (gid == shardcfg.Gid1)
		kv.frozen[i] = false
		kv.highestConfig[shardcfg.Tshid(i)] = 0
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
