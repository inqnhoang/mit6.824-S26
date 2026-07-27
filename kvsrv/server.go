package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu         sync.Mutex
	valueStore map[string]rpc.Record
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:         sync.Mutex{},
		valueStore: map[string]rpc.Record{},
	}

	return kv
}

func (kv *KVServer) Get(getArgs *rpc.GetArgs, getReply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if record, ok := kv.valueStore[getArgs.Key]; ok {
		getReply.Value = record.Value
		getReply.Version = record.Version
		getReply.Err = rpc.OK
	} else {
		getReply.Err = rpc.ErrNoKey
	}
}

func (kv *KVServer) Put(putArgs *rpc.PutArgs, putReply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if record, ok := kv.valueStore[putArgs.Key]; ok {
		if putArgs.Version == record.Version {
			kv.valueStore[putArgs.Key] = rpc.Record{Version: record.Version + 1, Value: putArgs.Value}
			putReply.Err = rpc.OK
		} else {
			putReply.Err = rpc.ErrVersion
		}
	} else {
		if putArgs.Version == 0 {
			kv.valueStore[putArgs.Key] = rpc.Record{Version: 1, Value: putArgs.Value}
			putReply.Err = rpc.OK
		} else {
			putReply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()

	return []any{kv}
}
