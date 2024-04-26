package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int64
	clientId int64
	sequenceNum int64
	empty bool
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.sequenceNum = 1
	ck.empty = true
	return ck
}

func (ck *Clerk) GetClientId() int64 {
	return ck.clientId
}

func (ck *Clerk) GetState() int {
	for {
		args := GetStateArgs{}
		reply := GetStateReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.GetState", &args, &reply)
		if ok && reply.IsLeader {
			DPrintf("Client %d GetState from %d", ck.clientId, ck.leaderId)
			return int(ck.leaderId)
		} else {
			time.Sleep(100 * time.Millisecond)
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		}
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key: key,
		ClientId: ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	reply := GetReply{}
	ck.GetState()
	DPrintf("Client %d Get %s from %d", ck.clientId, key, ck.leaderId)
	ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
	for !ok || reply.Err != OK {
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		ck.GetState()
		DPrintf("Client %d retry Get %s from %d value: %s reply.Err: %s", ck.clientId, key, ck.leaderId, reply.Value, reply.Err)
		ok = ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
	}
	if ok {
		ck.sequenceNum++
	}
	DPrintf("Client %d Return Get %s from %d", ck.clientId, key, ck.leaderId)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	reply := PutAppendReply{}
	ck.GetState()
	DPrintf("Client %d %s %s %s to %d", ck.clientId, op, key, value, ck.leaderId)
	ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
	for !ok || reply.Err != OK {
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		ck.GetState()
		DPrintf("Client %d %s %s %s to %d reply.Err: %s", ck.clientId, op, key, value, ck.leaderId, reply.Err)
		ok = ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
	}
	if ok {
		ck.sequenceNum++
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
