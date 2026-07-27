package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"math/rand"
	"sync"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	rcks map[tester.Tgid]*shardgrp.Clerk

	killed int32 // set by Kill()

	mu sync.Mutex
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	sck.mu = sync.Mutex{}
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	cfg_str, _, err := sck.IKVClerk.Get("Pending")
	if err != rpc.OK {
		return
	}
	pending := shardcfg.FromString(cfg_str)
	current := sck.Query()
	if pending.Num >= current.Num {
		sck.ChangeConfigTo(pending)
	}
}

func (sck *ShardCtrler) GetClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	sck.mu.Lock()
	defer sck.mu.Unlock()
	if clerk, ok := sck.rcks[gid]; ok {
		return clerk
	}
	clerk := shardgrp.MakeClerk(sck.clnt, servers)
	sck.rcks[gid] = clerk
	return clerk
}

func (sck *ShardCtrler) AssignClerks() {
	sck.mu.Lock()
	defer sck.mu.Unlock()

	cfg_str, _, err := sck.IKVClerk.Get("Gid1")
	if err != rpc.OK {
		return
	}
	config := shardcfg.FromString(cfg_str)

	for gid, srvs := range config.Groups {
		if _, ok := sck.rcks[gid]; !ok {
			sck.rcks[gid] = shardgrp.MakeClerk(sck.clnt, srvs)
		}
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	err := sck.IKVClerk.Put("Gid1", cfg.String(), 0)
	if err != rpc.OK {
		return
	}
	sck.AssignClerks()
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	stale := func() bool {
		return sck.Query().Num >= new.Num
	}

	sck.mu.Lock()
	for {
		cfgStr, ver, err := sck.IKVClerk.Get("Pending")
		// fmt.Printf("[ChangeConfigTo] Get Pending err=%v ver=%v newNum=%d\n", err, ver, new.Num)
		if err == rpc.ErrNoKey {
			perr := sck.IKVClerk.Put("Pending", new.String(), 0)
			// fmt.Printf("[ChangeConfigTo] Put Pending (new key) perr=%v newNum=%d\n", perr, new.Num)
			if perr == rpc.OK || perr == rpc.ErrMaybe {
				break
			}
			if perr == rpc.ErrVersion {
				time.Sleep(time.Duration(rand.Intn(20)+5) * time.Millisecond)
				continue
			}
			sck.mu.Unlock()
			return
		}
		if err != rpc.OK {
			// fmt.Printf("[ChangeConfigTo] Get Pending unexpected err=%v — bailing\n", err)
			sck.mu.Unlock()
			return
		}

		cur := shardcfg.FromString(cfgStr)
		if cur.Num < new.Num {
			perr := sck.IKVClerk.Put("Pending", new.String(), ver)
			// fmt.Printf("[ChangeConfigTo] Put Pending (cas) perr=%v newNum=%d curNum=%d\n", perr, new.Num, cur.Num)
			if perr == rpc.OK {
				break
			}
			if perr == rpc.ErrVersion || perr == rpc.ErrMaybe {
				continue
			}
			sck.mu.Unlock()
			return
		}
		// fmt.Printf("[ChangeConfigTo] superseded: cur.Num=%d >= new.Num=%d\n", cur.Num, new.Num)
		new = cur
		break
	}
	sck.mu.Unlock()

	for shardI := range shardcfg.NShards {
		current := sck.Query()
		if current.Num >= new.Num {
			return
		}
		shardOldg := current.Shards[shardI]
		shardNewg := new.Shards[shardI]

		if shardNewg == shardOldg {
			continue
		}

		var state []byte
		if shardOldg != 0 {
			oldClerk := sck.GetClerk(shardOldg, current.Groups[shardOldg])
			for {
				// fmt.Fprintf(os.Stderr, "[ChangeConfigTo] calling FreezeShard shard=%d num=%d gid=%d servers=%v\n",
				// shardI, new.Num, shardOldg, current.Groups[shardOldg])
				s, ferr := oldClerk.FreezeShard(shardcfg.Tshid(shardI), new.Num, stale)
				// fmt.Fprintf(os.Stderr, "[ChangeConfigTo] FreezeShard returned shard=%d num=%d err=%v\n", shardI, new.Num, ferr)
				if ferr == rpc.OK {
					state = s
					break
				}
				if ferr == rpc.ErrStale {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		if shardNewg != 0 {
			newClerk := sck.GetClerk(shardNewg, new.Groups[shardNewg])
			for {
				// fmt.Fprintf(os.Stderr, "[ChangeConfigTo] calling InstallShard shard=%d num=%d gid=%d servers=%v\n",
				// 	shardI, new.Num, shardOldg, current.Groups[shardOldg])
				ierr := newClerk.InstallShard(shardcfg.Tshid(shardI), state, new.Num, stale)
				// fmt.Fprintf(os.Stderr, "[ChangeConfigTo] InstallShard returned shard=%d num=%d err=%v\n", shardI, new.Num, ierr)
				if ierr == rpc.ErrStale {
					return
				}

				if ierr == rpc.OK {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		if shardOldg != 0 {
			oldClerk := sck.GetClerk(shardOldg, current.Groups[shardOldg])
			for {
				// fmt.Fprintf(os.Stderr, "[ChangeConfigTo] calling DeleteShard shard=%d num=%d gid=%d servers=%v\n",
				// 	shardI, new.Num, shardOldg, current.Groups[shardOldg])
				derr := oldClerk.DeleteShard(shardcfg.Tshid(shardI), new.Num, stale)
				// fmt.Fprintf(os.Stderr, "[ChangeConfigTo] DeleteShard returned shard=%d num=%d err=%v\n", shardI, new.Num, derr)
				if derr == rpc.ErrStale {
					return
				}

				if derr == rpc.OK {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	for {
		cfg_str, ver, err := sck.IKVClerk.Get("Gid1")
		// fmt.Printf("[ChangeConfigTo] final Get Gid1 err=%v ver=%v\n", err, ver)
		if err != rpc.OK {
			return
		}

		cfg := shardcfg.FromString(cfg_str)
		if new.Num <= cfg.Num {
			break
		}
		perr := sck.IKVClerk.Put("Gid1", new.String(), ver)
		// fmt.Printf("[ChangeConfigTo] final Put Gid1 num=%d perr=%v\n", new.Num, perr)
		if perr == rpc.OK {
			break
		}
		if perr == rpc.ErrVersion || perr == rpc.ErrMaybe {
			time.Sleep(time.Duration(rand.Intn(20)+5) * time.Millisecond)
			continue
		}
		return
	}
	sck.AssignClerks()
	// fmt.Printf("[ChangeConfigTo] done num=%d\n", new.Num)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	config_str, _, err := sck.IKVClerk.Get("Gid1")
	if err != rpc.OK {
		return shardcfg.MakeShardConfig()
	}

	return shardcfg.FromString(config_str)
}
