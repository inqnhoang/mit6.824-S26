package rpc

type Err string

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	ErrMaybe = "ErrMaybe"

	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"

	ErrStale  = "ErrStale"
	ErrFrozen = "ErrFrozen"
)

type Tversion uint64

type Record struct {
	Version Tversion
	Value   string
}
type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}
