package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"fmt"
	"time"
)

const Unlocked = "unlocked"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	clientId string
	lockKey  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, clientId: kvtest.RandValue(8), lockKey: l}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// get the key to check its value
		value, ver, err := lk.ck.Get(lk.lockKey)
		if value == lk.clientId {
			return
		}

		if value == Unlocked || err == rpc.ErrNoKey {
			// try to put
			err = lk.ck.Put(lk.lockKey, lk.clientId, ver)

			if err == rpc.OK {
				return
			}

			if err == rpc.ErrMaybe {
				// recheck
				value, _, _ = lk.ck.Get(lk.lockKey)
				if value == lk.clientId {
					return
				}
			}
		}

		time.Sleep(300 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	value, version, err := lk.ck.Get(lk.lockKey)
	// should throw error
	if err == rpc.ErrNoKey {
		panic("Lock doesn't exist")
	}
	if value != lk.clientId {
		panic("Cannot release lock if not already hold it")
	}

	err = lk.ck.Put(lk.lockKey, Unlocked, version)

	switch err {
	case rpc.OK:
		return
	case rpc.ErrVersion:
		panic("Cannot release lock due to version mismatch")
	case rpc.ErrNoKey:
		panic("Cannot release lock due to key mismatch")
	case rpc.ErrMaybe:
		// Treat as released; can't confirm but that's OK — lock is now raceable
		return
	default:
		panic(fmt.Sprintf("Unexpected error on release: %v", err))
	}
}
