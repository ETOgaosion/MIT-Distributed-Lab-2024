package kvraft

const (
	OK             	= "OK"
	ErrNoKey       	= "ErrNoKey"
	ErrWrongLeader 	= "ErrWrongLeader"
	ErrTimeout		= "ErrTimeout"
	ErrCmd			= "ErrCmd"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SequenceNum int64
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SequenceNum int64
}

type GetReply struct {
	Err   string
	Value string
}

type IsLeaderArgs struct {
}

type IsLeaderReply struct {
	IsLeader bool
}

const (
	GetCmd = 1
	PutCmd = 2
	AppendCmd = 3
)