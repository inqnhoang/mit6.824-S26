package kvraft

import (
	"bytes"
	"fmt"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
	mu  sync.Mutex

	valueStore map[string]rpc.Record
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		if record, ok := kv.valueStore[op.Key]; ok {
			reply.Value = record.Value
			reply.Version = record.Version
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply

	case rpc.PutArgs:
		reply := rpc.PutReply{}
		if record, ok := kv.valueStore[op.Key]; ok {
			if op.Version == record.Version {
				kv.valueStore[op.Key] = rpc.Record{Version: record.Version + 1, Value: op.Value}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
			// fmt.Printf("[DoOp %d] Put key=%s found=%v curVer=%v reqVer=%v -> %v\n", kv.me, op.Key, ok, record.Version, op.Version, reply.Err)

		} else {
			if op.Version == 0 {
				kv.valueStore[op.Key] = rpc.Record{Version: 1, Value: op.Value}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
			// fmt.Printf("[DoOp %d] Put key=%s found=%v curVer=%v reqVer=%v -> %v\n", kv.me, op.Key, ok, record.Version, op.Version, reply.Err)
		}
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.mu.Lock()
	e.Encode(kv.valueStore)
	kv.mu.Unlock()
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	d.Decode(&kv.valueStore)
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, valueStore: make(map[string]rpc.Record), mu: sync.Mutex{}}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}

func (kv *KVServer) reached(peer int) {
	fmt.Printf("\n%d reached ==========================\n", peer)
}
