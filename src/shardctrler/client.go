package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientId int64
	SequenceNum int64
	LeaderId int64
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
	// Your code here.
	ck.ClientId = nrand()
	ck.LeaderId = 0
	ck.SequenceNum = 1
	return ck
}

func (ck *Clerk) GetState() int {
	for {
		args := GetStateArgs{}
		reply := GetStateReply{}
		ok := ck.servers[ck.LeaderId].Call("ShardCtrler.GetState", &args, &reply)
		if ok && reply.IsLeader {
			DPrintf("Client %d GetState from %d", ck.ClientId, ck.LeaderId)
			return int(ck.LeaderId)
		} else {
			time.Sleep(100 * time.Millisecond)
			ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
		}
	}
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum
	var reply QueryReply

	ck.GetState()
	DPrintf("Client %d Query %d from %d", ck.ClientId, num, ck.LeaderId)
	ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Query", &args, &reply)
	for !ok || reply.Err != OK {
		ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
		ck.GetState()
		ok = ck.servers[ck.LeaderId].Call("ShardCtrler.Query", &args, &reply)
		DPrintf("Client %d retry Query %d from %d reply.Err: %s", ck.ClientId, num, ck.LeaderId, reply.Err)
	}
	if ok {
		ck.SequenceNum++
	}
	DPrintf("Client %d Return Query %d from %d", ck.ClientId, num, ck.LeaderId)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum
	var reply JoinReply

	ck.GetState()
	DPrintf("Client %d Join %v from %d", ck.ClientId, servers, ck.LeaderId)
	ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Join", &args, &reply)
	for !ok || reply.Err != OK {
		ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
		ck.GetState()
		DPrintf("Client %d retry Join %v from %d reply.Err: %s", ck.ClientId, servers, ck.LeaderId, reply.Err)
		ok = ck.servers[ck.LeaderId].Call("ShardCtrler.Join", &args, &reply)
	}
	if ok {
		ck.SequenceNum++
	}
	DPrintf("Client %d Return Join %v from %d", ck.ClientId, servers, ck.LeaderId)
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum
	var reply LeaveReply

	ck.GetState()
	DPrintf("Client %d Leave %v from %d", ck.ClientId, gids, ck.LeaderId)
	ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Leave", &args, &reply)
	for !ok || reply.Err != OK {
		ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
		ck.GetState()
		DPrintf("Client %d retry Leave %v from %d reply.Err: %s", ck.ClientId, gids, ck.LeaderId, reply.Err)
		ok = ck.servers[ck.LeaderId].Call("ShardCtrler.Leave", &args, &reply)
	}
	if ok {
		ck.SequenceNum++
	}
	DPrintf("Client %d Return Leave %vfrom %d", ck.ClientId, gids, ck.LeaderId)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum
	var reply LeaveReply

	ck.GetState()
	DPrintf("Client %d Move %v %v from %d", ck.ClientId, shard, gid, ck.LeaderId)
	ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Move", &args, &reply)
	for !ok || reply.Err != OK {
		ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
		ck.GetState()
		DPrintf("Client %d retry Move %v %v from %d reply.Err: %s", ck.ClientId, shard, gid, ck.LeaderId, reply.Err)
		ok = ck.servers[ck.LeaderId].Call("ShardCtrler.Move", &args, &reply)
	}
	if ok {
		ck.SequenceNum++
	}
	DPrintf("Client %d Return Move %v %vfrom %d", ck.ClientId, shard, gid, ck.LeaderId)
}
