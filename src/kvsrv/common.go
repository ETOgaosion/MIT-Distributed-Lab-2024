package kvsrv

// Client ID request
type ClientIDArgs struct {
	Retry bool
}

type ClientIDReply struct {
	ClientId int
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int
	PutAppendReqId int
	Retry bool
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
