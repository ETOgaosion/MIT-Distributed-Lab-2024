package kvraft

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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv map[string]string
	clientSequences map[int64]int64
	waitChannels map[int64]chan OpReply

	persister *raft.Persister
	currentBytes int

}


// common action for Get, Put, Append, wait for engine extract raft's output and manage with kv storage
func (kv *KVServer) waitCmd(cmd Op) OpReply {
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

// for faster leader recognization
func (kv *KVServer) GetState(args *GetStateArgs, reply *GetStateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	reply.IsLeader = isLeader
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
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := kv.waitCmd(cmd)
	reply.Err, reply.Value = res.Err, res.Value
	DPrintf("Server %d Get reply: key: %s, client ID: %d, sequence num: %d, err: %s, value: %s", kv.me, args.Key, args.ClientId, args.SequenceNum, reply.Err, reply.Value)
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

// should be in lock
func (kv *KVServer) isRepeated(clientId, sequenceNum int64) bool {
	seq, ok := kv.clientSequences[clientId]
	if ok && seq >= sequenceNum {
		DPrintf("Server %v receive repeated cmd from ClientId: %v sequenceNum: %v seq: %v", kv.me, clientId, sequenceNum, seq)
		return true
	}
	return false
}

// should be in lock
func (kv *KVServer) getSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientSequences)
	e.Encode(kv.kv)
	snapshot := w.Bytes()
	return snapshot
}

// should be in lock
func (kv *KVServer) readSnapShot(data []byte) {
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

func (kv *KVServer) engineStart() {
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
	kv.waitChannels = make(map[int64]chan OpReply)
	kv.persister = persister
	kv.currentBytes = 0
	kv.readSnapShot(kv.persister.ReadSnapshot())

	go kv.engineStart()

	return kv
}
