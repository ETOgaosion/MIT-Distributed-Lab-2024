package shardkv

import (
	"time"

	"6.5840/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             		= "OK"
	ErrNoKey       		= "ErrNoKey"
	ErrWrongGroup  		= "ErrWrongGroup"
	ErrWrongLeader 		= "ErrWrongLeader"
	ErrTimeout     		= "ErrTimeout"
	ErrShardNotReady 	= "ErrShardNotReady"
	ErrCmd				= "ErrCmd"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
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
	Err   string
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

type SendShardArgs struct {
	Num int
	Dest []string
	Src []string
	ClientSequences map[int64]int64
	Data map[string]string
}

type SendShardReply struct {
	Err string
}

type DeleteShardArgs struct {
	Num int
	Dest []string
	Src []string
	Shards []int
	Keys []string
}

type DeleteShardReply struct {
	Err string
}

const (
	GetCmd = 1
	PutCmd = 2
	AppendCmd = 3
)

type CmdType int8

const (
	Serving = 1
	Pulling = 2
	Offering = 3
)

type ShardNodeState int8

const (
	UpdateConfigInterval = 100 * time.Millisecond
	SendShardsInterval = 150 * time.Millisecond
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func tableDeepCopy(src map[string]string) map[string]string {
	dst := make(map[string]string)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}