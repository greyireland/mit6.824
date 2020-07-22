package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// OpPut or OpAppend
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "OpPut" or "OpAppend"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id    int64
	ReqId int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
	ReqId int
}

type GetReply struct {
	WrongLeader bool
	Err   Err
	Value string
}
