package lock

import (
	"time"

	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
	id       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, id: kvtest.RandValue(8)}
	ck.Put(lockname, "", 0)
	return lk
}

func (lk *Lock) Acquire() {
	value, version, _ := lk.ck.Get(lk.lockname)

	for {
		if value == lk.id {
			return
		}

		if value == "" {
			lk.ck.Put(lk.lockname, lk.id, version)
		}
		value, version, _ = lk.ck.Get(lk.lockname)
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	value, version, _ := lk.ck.Get(lk.lockname)

	if value == lk.id {
		lk.ck.Put(lk.lockname, "", version)
	}
}
