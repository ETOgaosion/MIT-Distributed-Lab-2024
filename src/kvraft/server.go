package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CmdType int8
	ClientId int64
	SequenceNum int64
	Key string
	Value string
}

type OpReply struct {
	Err Err
	LeaderId int64
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	kv map[string]string
	clientSequences map[int64]int64
	waitCh map[int64]chan OpReply

}

func (kv *KVServer) waitCmd(cmd Op) OpReply {
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply := OpReply{
			Err: ErrWrongLeader,
			LeaderId: int64(kv.rf.GetCurrentLeader()),
		}
		return reply
	}
	kv.mu.Lock()
	ch := make(chan OpReply, 1)
	kv.waitCh[cmd.ClientId] = ch
	kv.mu.Unlock()
	
	select {
	case res := <-ch:
		kv.mu.Lock()
		delete(kv.waitCh, cmd.ClientId)
		kv.mu.Unlock()
		return res
	case <-time.After(100 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitCh, cmd.ClientId)
		kv.mu.Unlock()
		res := OpReply{
			Err: ErrTimeout,
		}
		return res
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server %d Get request: key: %s, client ID: %d, sequence num: %d", kv.me, args.Key, args.ClientId, args.SequenceNum)
	kv.mu.Lock()
	if args.SequenceNum <= kv.clientSequences[args.ClientId] {
		reply.Value = kv.kv[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	cmd := Op {
		CmdType: GetCmd,
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		Key: args.Key,
	}
	res := kv.waitCmd(cmd)
	reply.Err, reply.Value, reply.LeaderId = res.Err, res.Value, res.LeaderId
	DPrintf("Server %d Get reply: key: %s, value: %s, client ID: %d, sequence num: %d, err: %s, value: %s, leader ID: %d", kv.me, args.Key, reply.Value, args.ClientId, args.SequenceNum, reply.Err, reply.Value, reply.LeaderId)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Server %d %s request: key: %s, value: %s, client ID: %d, sequence num: %d", kv.me, args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum)
	kv.mu.Lock()
	if args.SequenceNum <= kv.clientSequences[args.ClientId] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	cmd := Op {
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		Key: args.Key,
		Value: args.Value,
	}
	if args.Op == "Put" {
		cmd.CmdType = PutCmd
	} else if args.Op == "Append" {
		cmd.CmdType = AppendCmd
	} else {
		reply.Err = ErrCmd
		return
	}
	res := kv.waitCmd(cmd)
	reply.Err, reply.LeaderId = res.Err, res.LeaderId
	DPrintf("Server %d %s reply: key: %s, value: %s, client ID: %d, sequence num: %d, err: %s, leader ID: %d", kv.me, args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum, reply.Err, reply.LeaderId)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) isRepeated(clientId, sequenceNum int64) bool {
	seq, ok := kv.clientSequences[clientId]
	if ok && seq >= sequenceNum {
		return true
	}
	return false
}

func (kv *KVServer) engineStart() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()
			if op.CmdType == GetCmd {
				clientId := op.ClientId
				if op.SequenceNum > kv.clientSequences[clientId] {
					// I think this is not gonna happen
					kv.clientSequences[clientId] = op.SequenceNum
				}
				_, ok := kv.waitCh[clientId]
				if ok && kv.clientSequences[clientId] == op.SequenceNum {
					res := OpReply {
						Err: OK,
						Value: kv.kv[op.Key],
						LeaderId: int64(kv.rf.GetCurrentLeader()),
					}
					select {
					case kv.waitCh[clientId] <- res:
					}
				}
			} else {
				if !kv.isRepeated(op.ClientId, op.SequenceNum) {
					if op.CmdType == PutCmd {
						kv.kv[op.Key] = op.Value
					} else if op.CmdType == AppendCmd {
						kv.kv[op.Key] += op.Value
					}
					kv.clientSequences[op.ClientId] = op.SequenceNum
				}
				_, ok := kv.waitCh[op.ClientId]
				if ok && kv.clientSequences[op.ClientId] == op.SequenceNum {
					res := OpReply {
						Err: OK,
						LeaderId: int64(kv.rf.GetCurrentLeader()),
					}
					select {
					case kv.waitCh[op.ClientId] <- res:
					}
				}
			}
			kv.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientSequences = make(map[int64]int64)
	kv.kv = make(map[string]string)
	kv.waitCh = make(map[int64]chan OpReply)

	go kv.engineStart()

	return kv
}
