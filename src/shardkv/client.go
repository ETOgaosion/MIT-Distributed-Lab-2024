package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm *shardctrler.Clerk
	config shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIds map[int]int64
	clientId int64
	sequenceNum int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leaderIds = make(map[int]int64)
	ck.clientId = nrand()
	ck.sequenceNum = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.SequenceNum = ck.sequenceNum

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			if leader, ok := ck.leaderIds[gid]; !ok {
				leader_srv := ck.make_end(servers[leader])
				var reply GetReply
				ok := leader_srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.sequenceNum++
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					ck.leaderIds[gid] = (leader + 1) % int64(len(servers))
					continue
				}
				ck.leaderIds[gid] = (leader + 1) % int64(len(servers))
			} else {
				ck.leaderIds[gid] = 0
			}
			for si := 0; si < len(servers) - 1; si++ {
				srv := ck.make_end(servers[(int(ck.leaderIds[gid]) + si) % len(servers)])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.sequenceNum++
					ck.leaderIds[gid] = (ck.leaderIds[gid] + int64(si)) % int64(len(servers))
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.SequenceNum = ck.sequenceNum

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if leader, ok := ck.leaderIds[gid]; !ok {
				leader_srv := ck.make_end(servers[leader])
				var reply GetReply
				ok := leader_srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.sequenceNum++
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					ck.leaderIds[gid] = (leader + 1) % int64(len(servers))
					continue
				}
				ck.leaderIds[gid] = (leader + 1) % int64(len(servers))
			} else {
				ck.leaderIds[gid] = 0
			}
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[(int(ck.leaderIds[gid]) + si) % len(servers)])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.sequenceNum++
					ck.leaderIds[gid] = (ck.leaderIds[gid] + int64(si)) % int64(len(servers))
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
