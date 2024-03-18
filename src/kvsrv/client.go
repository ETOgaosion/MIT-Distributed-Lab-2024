package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	id int
	appendcounter int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.appendcounter = 0
	// You'll have to add code here.
	return ck
}

// get current client id
func (ck *Clerk) GetClientID() int {
	args := ClientIDArgs{}
	args.Retry = false
	reply := ClientIDReply{}
	ok := ck.server.Call("KVServer.GetClientID", &args, &reply)
	for !ok {
		args.Retry = true
		ok = ck.server.Call("KVServer.GetClientID", &args, &reply)
	}
	ck.id = reply.ClientID
	return reply.ClientID
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}
	args.Key = key
	reply := GetReply{}
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}
	args.Key = ""
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Retry = false
	args.ClientID = ck.id
	if op == "Append" {
		args.PutAppendReqID = ck.appendcounter
		ck.appendcounter++
	}
	reply := PutAppendReply{}
	ok := ck.server.Call("KVServer." + op, &args, &reply)
	for !ok {
		args.Retry = true
		ok = ck.server.Call("KVServer." + op, &args, &reply)
	}
	args.Key = ""
	args.Value = ""
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	args := PutAppendArgs{}
	args.Key = key
	reply := PutAppendReply{}
	ok := ck.server.Call("KVServer.Put", &args, &reply)
	for !ok {
		args.Retry = true
		ok = ck.server.Call("KVServer.Put", &args, &reply)
	}
	args.Key = ""
	args.Value = ""
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Retry = false
	args.ClientID = ck.id
	args.PutAppendReqID = ck.appendcounter
	ck.appendcounter++
	reply := PutAppendReply{}
	ok := ck.server.Call("KVServer.Append", &args, &reply)
	for !ok {
		args.Retry = true
		ok = ck.server.Call("KVServer.Append", &args, &reply)
	}
	args.Key = ""
	args.Value = ""
	return reply.Value
}
