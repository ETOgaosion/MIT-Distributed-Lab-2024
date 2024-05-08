package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"github.com/linkdata/deadlock"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientId int64
	SequenceNum int64
	LeaderId int64
	mu deadlock.Mutex
}

const (
	retryInterval = 300 * time.Microsecond
)

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

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum
	var reply QueryReply

	ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Query", &args, &reply)
	DPrintf("Client %d Query %d from %d", ck.ClientId, num, ck.LeaderId)
	for !ok || reply.Err != OK {
		ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
		ok = ck.servers[ck.LeaderId].Call("ShardCtrler.Query", &args, &reply)
		DPrintf("Client %d retry Query %d from %d ok: %v reply.Err: %s len(ck.servers): %v", ck.ClientId, num, ck.LeaderId, ok, reply.Err, len(ck.servers))
		time.Sleep(retryInterval)
	}
	if ok {
		ck.SequenceNum++
	}
	DPrintf("Client %d Return Query %d from %d", ck.ClientId, num, ck.LeaderId)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum
	var reply JoinReply

	ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Join", &args, &reply)
	DPrintf("Client %d Join %v from %d", ck.ClientId, servers, ck.LeaderId)
	for !ok || reply.Err != OK {
		ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
		ok = ck.servers[ck.LeaderId].Call("ShardCtrler.Join", &args, &reply)
		DPrintf("Client %d retry Join %v from %d reply.Err: %s", ck.ClientId, servers, ck.LeaderId, reply.Err)
		time.Sleep(retryInterval)
	}
	if ok {
		ck.SequenceNum++
	}
	DPrintf("Client %d Return Join %v from %d", ck.ClientId, servers, ck.LeaderId)
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum
	var reply LeaveReply

	ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Leave", &args, &reply)
	DPrintf("Client %d Leave %v from %d", ck.ClientId, gids, ck.LeaderId)
	for !ok || reply.Err != OK {
		ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
		ok = ck.servers[ck.LeaderId].Call("ShardCtrler.Leave", &args, &reply)
		DPrintf("Client %d retry Leave %v from %d reply.Err: %s", ck.ClientId, gids, ck.LeaderId, reply.Err)
		time.Sleep(retryInterval)
	}
	if ok {
		ck.SequenceNum++
	}
	DPrintf("Client %d Return Leave %vfrom %d", ck.ClientId, gids, ck.LeaderId)
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.ClientId
	args.SequenceNum = ck.SequenceNum
	var reply LeaveReply

	ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Move", &args, &reply)
	DPrintf("Client %d Move %v %v from %d", ck.ClientId, shard, gid, ck.LeaderId)
	for !ok || reply.Err != OK {
		ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
		ok = ck.servers[ck.LeaderId].Call("ShardCtrler.Move", &args, &reply)
		DPrintf("Client %d retry Move %v %v from %d reply.Err: %s", ck.ClientId, shard, gid, ck.LeaderId, reply.Err)
		time.Sleep(retryInterval)
	}
	if ok {
		ck.SequenceNum++
	}
	DPrintf("Client %d Return Move %v %vfrom %d", ck.ClientId, shard, gid, ck.LeaderId)
}
