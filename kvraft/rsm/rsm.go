package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Me  int
	Id  int
	Req any
}

type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

func (rsm *RSM) reader() {
	for applyMsg := range rsm.applyCh {
		if applyMsg.SnapshotValid {
			rsm.sm.Restore(applyMsg.Snapshot)
		}

		if !applyMsg.CommandValid {
			continue
		}

		op := applyMsg.Command.(Op)
		res := rsm.sm.DoOp(op.Req)
		op.Req = res

		rsm.mu.Lock()
		ch, ok := rsm.pending[applyMsg.CommandIndex]
		delete(rsm.pending, applyMsg.CommandIndex)
		rsm.mu.Unlock()

		if ok {
			ch <- op
		}

		if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
			bytes := rsm.sm.Snapshot()
			rsm.rf.Snapshot(applyMsg.CommandIndex, bytes)
		}
	}
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int
	sm           StateMachine

	opId    int
	pending map[int]chan Op
}

func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		opId:         0,
		pending:      make(map[int]chan Op),
	}

	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	op := Op{
		Me:  rsm.me,
		Id:  rsm.opId,
		Req: req,
	}
	rsm.opId += 1

	index, term, isLeader := rsm.rf.Start(op)

	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan Op, 1)
	rsm.pending[index] = ch
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.pending, index)
		rsm.mu.Unlock()
	}()

	for {
		select {
		case appliedOp := <-ch:
			if appliedOp.Me != op.Me || appliedOp.Id != op.Id {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, appliedOp.Req
		case <-time.After(150 * time.Millisecond):
			curTerm, curLeader := rsm.rf.GetState()
			if !curLeader || curTerm != term {
				return rpc.ErrMaybe, nil
			}
		}
	}
}
