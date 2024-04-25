package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int64
	clientId int64
	sequenceNum int64
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
	return ck
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
	DPrintf("Client %d Get %s from %d", ck.clientId, key, ck.leaderId)
	ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
	for !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		if reply.LeaderId != -1 {
			ck.leaderId = reply.LeaderId;
		} else {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		}
		DPrintf("Client %d Get %s from %d", ck.clientId, key, ck.leaderId)
		ok = ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
	}
	ck.sequenceNum++
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
	DPrintf("Client %d %s %s %s to %d", ck.clientId, op, key, value, ck.leaderId)
	ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
	for !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		if reply.LeaderId != -1 {
			ck.leaderId = reply.LeaderId
		} else {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		}
		DPrintf("Client %d %s %s %s to %d", ck.clientId, op, key, value, ck.leaderId)
		ok = ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
	}
	ck.sequenceNum++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
