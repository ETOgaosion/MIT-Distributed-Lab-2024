package shardkv

import (
	"bytes"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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
	CmdType int8
	ClientId int64
	SequenceNum int64
	Err string
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead    int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	mck          *shardctrler.Clerk

	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv map[string]string
	clientSequences map[int64]int64
	waitChannels map[int64]chan OpReply

	lastConfig shardctrler.Config
	curConfig shardctrler.Config
	shardStates map[int]int8

	persister *raft.Persister
	currentBytes int
}

// for faster leader recognization
func (kv *ShardKV) isLeader() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	return isLeader
}

// common action for Get, Put, Append, wait for engine extract raft's output and manage with kv storage
func (kv *ShardKV) waitCmd(cmd Op) OpReply {
	kv.mu.Lock()
	ch := make(chan OpReply, 1)
	kv.waitChannels[cmd.ClientId] = ch
	kv.mu.Unlock()
	
	select {
	case res := <-ch:
		if res.CmdType != cmd.CmdType || res.ClientId != cmd.ClientId || res.SequenceNum != cmd.SequenceNum {
			res.Err = ErrCmd
		}
		kv.mu.Lock()
		delete(kv.waitChannels, cmd.ClientId)
		kv.mu.Unlock()
		return res
	case <-time.After(100 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitChannels, cmd.ClientId)
		kv.mu.Unlock()
		res := OpReply{
			Err: ErrTimeout,
		}
		return res
	}
}

// should be in lock
// return (valid, ready)
func (kv *ShardKV) checkShard(key string) (bool, bool) {
	shard := key2shard(key)
	if kv.curConfig.Shards[shard] != kv.gid {
		return false, false
	}
	if kv.shardStates[shard] != Serving {
		return true, false
	}
	return true, true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server %d Get request: key: %s, client ID: %d, sequence num: %d", kv.me, args.Key, args.ClientId, args.SequenceNum)
	kv.mu.Lock()
	valid, ready := kv.checkShard(args.Key)
	if !valid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if !ready {
		reply.Err = ErrShardNotReady
		kv.mu.Unlock()
		return
	}
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
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := kv.waitCmd(cmd)
	reply.Err, reply.Value = res.Err, res.Value
	DPrintf("Server %d Get reply: key: %s, client ID: %d, sequence num: %d, err: %s, value: %s", kv.me, args.Key, args.ClientId, args.SequenceNum, reply.Err, reply.Value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Server %d %s request: key: %s, value: %s, client ID: %d, sequence num: %d", kv.me, args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum)
	kv.mu.Lock()
	valid, ready := kv.checkShard(args.Key)
	if !valid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if !ready {
		reply.Err = ErrShardNotReady
		kv.mu.Unlock()
		return
	}
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
		log.Fatalf("Invalid operation: %s", args.Op)
		return
	}
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := kv.waitCmd(cmd)
	reply.Err = res.Err
	DPrintf("Server %d %s reply: key: %s, value: %s, client ID: %d, sequence num: %d, err: %s", kv.me, args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum, reply.Err)
}

func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	reply.Err = OK

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// should be in lock
func (kv *ShardKV) isRepeated(clientId, sequenceNum int64) bool {
	seq, ok := kv.clientSequences[clientId]
	if ok && seq >= sequenceNum {
		DPrintf("Server %v receive repeated cmd from ClientId: %v sequenceNum: %v seq: %v", kv.me, clientId, sequenceNum, seq)
		return true
	}
	return false
}

// should be in lock
func (kv *ShardKV) getSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientSequences)
	e.Encode(kv.kv)
	snapshot := w.Bytes()
	return snapshot
}

// should be in lock
func (kv *ShardKV) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	e_decode_clientseq := d.Decode(&kv.clientSequences)
	e_decode_kv := d.Decode(&kv.kv)
	has_err := false
	if e_decode_clientseq != nil {
		has_err = true
		log.Printf("decode clientseq error %v", e_decode_clientseq)
	}
	if e_decode_kv != nil {
		has_err = true
		log.Printf("decode kv error %v", e_decode_kv)
	}
	if has_err {
		debug.PrintStack()
		os.Exit(-1)
	}
}

func (kv *ShardKV) engineStart() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			clientId := op.ClientId
			kv.mu.Lock()
			// NOTICE: unsafe.Sizeof do not count the actual size of ptr types
			kv.currentBytes += int(unsafe.Sizeof(op)) + len(op.Key) + len(op.Value)
			if op.SequenceNum < kv.clientSequences[clientId] {
				continue
			}
			if op.CmdType == PutCmd || op.CmdType == AppendCmd {
				if !kv.isRepeated(clientId, op.SequenceNum) {
					if op.CmdType == PutCmd {
						kv.kv[op.Key] = op.Value
					} else if op.CmdType == AppendCmd {
						kv.kv[op.Key] += op.Value
					}
				}
			}
			if op.SequenceNum > kv.clientSequences[clientId] {
				kv.clientSequences[clientId] = op.SequenceNum
			}
			_, ok := kv.waitChannels[clientId]
			if ok {
				res := OpReply {
					CmdType: op.CmdType,
					ClientId: op.ClientId,
					SequenceNum: op.SequenceNum,
					Err: OK,
					Value: kv.kv[op.Key],
				}
				DPrintf("Server %d reply: key: %s, client ID: %d, sequence num: %d, err: %s, value: %s", kv.me, op.Key, op.ClientId, op.SequenceNum, res.Err, res.Value)
				kv.waitChannels[clientId] <- res
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate && kv.currentBytes > kv.maxraftstate {
				DPrintf("Server %d start snapshot, kv.persister.RaftStateSize(): %v, kv.currentBytes: %v, kv.maxraftstate: %v", kv.me, kv.persister.RaftStateSize(), kv.currentBytes, kv.maxraftstate)
				snapshot := kv.getSnapShot()
				kv.currentBytes = 0
				kv.mu.Unlock()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			} else {
				kv.mu.Unlock()
			}
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapShot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.kv = make(map[string]string)
	kv.clientSequences = make(map[int64]int64)
	kv.waitChannels = make(map[int64]chan OpReply)
	kv.persister = persister
	kv.currentBytes = 0

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.engineStart()

	return kv
}
