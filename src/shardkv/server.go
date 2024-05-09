package shardkv

import (
	"bytes"
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"github.com/linkdata/deadlock"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CmdType int8
	// Get/Put/Append
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
	mu           deadlock.Mutex
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
}

// for faster leader recognization
// should be in lock
func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

// common action for Get, Put, Append, wait for engine extract raft's output and manage with kv storage
func (kv *ShardKV) waitCmd(index int64, cmd Op) OpReply {
	ch := make(chan OpReply, 1)
	kv.mu.Lock()
	kv.waitChannels[index] = ch
	kv.mu.Unlock()
	
	select {
	case res := <-ch:
		if res.CmdType != cmd.CmdType || res.ClientId != cmd.ClientId || res.SequenceNum != cmd.SequenceNum {
			res.Err = ErrCmd
		}
		kv.mu.Lock()
		delete(kv.waitChannels, index)
		kv.mu.Unlock()
		return res
	case <-time.After(500 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitChannels, index)
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
		return false, true
	}
	if kv.shardStates[shard] != Serving {
		return true, false
	}
	return true, true
}

// =================== RPC Receivers ==================
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server %v %p gid: %d (isLeader: %v) Get request: key: %s, client ID: %d, sequence num: %d", kv.me, kv, kv.gid, kv.isLeader(), args.Key, args.ClientId, args.SequenceNum)
	kv.mu.Lock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		DPrintf("Server %v %p gid: %d (isLeader: %v) Get reply: key: %s, client ID: %d, sequence num: %d, err: %s", kv.me, kv, kv.gid, kv.isLeader(), args.Key, args.ClientId, args.SequenceNum, reply.Err)
		kv.mu.Unlock()
		return
	}
	valid, ready := kv.checkShard(args.Key)
	if !valid {
		reply.Err = ErrWrongGroup
		DPrintf("Server %v %p gid: %d (isLeader: %v) Get reply: key: %s, client ID: %d, sequence num: %d, err: %s", kv.me, kv, kv.gid, kv.isLeader(), args.Key, args.ClientId, args.SequenceNum, reply.Err)
		kv.mu.Unlock()
		return
	}
	if !ready {
		reply.Err = ErrShardNotReady
		DPrintf("Server %v %p gid: %d (isLeader: %v) Get reply: key: %s, client ID: %d, sequence num: %d, err: %s", kv.me, kv, kv.gid, kv.isLeader(), args.Key, args.ClientId, args.SequenceNum, reply.Err)
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
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := kv.waitCmd(int64(index), cmd)
	reply.Err, reply.Value = res.Err, res.Value
	DPrintf("Server %v %p gid: %d (isLeader: %v) Get reply: key: %s, client ID: %d, sequence num: %d, err: %s, value: %s", kv.me, kv, kv.gid, kv.isLeader(), args.Key, args.ClientId, args.SequenceNum, reply.Err, reply.Value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Server %v %p gid: %d (isLeader: %v) %s request: key: %s, value: %s, client ID: %d, sequence num: %d, isLeader: %v", kv.me, kv, kv.gid, kv.isLeader(), args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum, kv.isLeader())
	kv.mu.Lock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
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
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := kv.waitCmd(int64(index), cmd)
	reply.Err = res.Err
	DPrintf("Server %v %p gid: %d (isLeader: %v) %s reply: key: %s, value: %s, client ID: %d, sequence num: %d, err: %s", kv.me, kv, kv.gid, kv.isLeader(), args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum, reply.Err)
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	// Your code here.
	D2Printf("Server %v %p gid: %d (isLeader: %v) receive install shard request: num: %v, dest: %v, src: %v, shards: %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args.Dest, args.Src, args.Shards)
	kv.mu.Lock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		D2Printf("Server %v %p gid: %d (isLeader: %v) reply install shard request: num: %v, dest: %v, src: %v, shards: %v, err: %s", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args.Dest, args.Src, args.Shards, reply.Err)
		kv.mu.Unlock()
		return
	}
	if kv.curConfig.Num < args.Num {
		// we fall behind others, let them retry later, do not delete them
		reply.Err = OK
		D2Printf("Server %v %p gid: %d (isLeader: %v) reply install shard request: num: %v, dest: %v, src: %v, shards: %v, err: %s kv.curConfig.Num %v < args.Num %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args.Dest, args.Src, args.Shards, reply.Err, kv.curConfig.Num, args.Num)
		kv.mu.Unlock()
		return
	}
	if kv.curConfig.Num > args.Num {
		// we fall behind others, let them retry later, do not delete them
		reply.Err = OK
		D2Printf("Server %v %p gid: %d (isLeader: %v) reply install shard request: num: %v, dest: %v, src: %v, shards: %v, err: %s kv.curConfig.Num %v > args.Num %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args.Dest, args.Src, args.Shards, reply.Err, kv.curConfig.Num, args.Num)
		kv.mu.Unlock()
		deleteArgs := kv.generateDeleteShardArgs(args)
		go kv.sendDeleteShard(&deleteArgs)
		return
	}
	for _, shard := range args.Shards {
		if kv.shardStates[shard] != Pulling {
			reply.Err = OK
			D2Printf("Server %v %p gid: %d (isLeader: %v) reply install shard request: num: %v, dest: %v, src: %v, shards: %v, err: %s, shard %v state %v != Pulling", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args.Dest, args.Src, args.Shards, reply.Err, shard, kv.shardStates[shard])
			kv.mu.Unlock()
			deleteArgs := kv.generateDeleteShardArgs(args)
			go kv.sendDeleteShard(&deleteArgs)
			return
		}
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(*args)
	DPrintf("Server %v %p gid: %d (isLeader: %v) start install shard %v index %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan OpReply, 1)
	kv.mu.Lock()
	kv.waitChannels[int64(index)] = ch
	kv.mu.Unlock()
	select {
	case res := <-ch:
		if res.Err == OK {
			reply.Err = OK
			deleteArgs := kv.generateDeleteShardArgs(args)
			go kv.sendDeleteShard(&deleteArgs)
			kv.mu.Lock()
			delete(kv.waitChannels, int64(index))
			kv.mu.Unlock()
			return
		} else {
			reply.Err = res.Err
			kv.mu.Lock()
			delete(kv.waitChannels, int64(index))
			kv.mu.Unlock()
			return
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
		return
	}
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	D2Printf("Server %v %p gid: %d (isLeader: %v) receive delete shard request: num: %v, dest: %v, src: %v, shards: %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args.Dest, args.Src, args.Shards)
	kv.mu.Lock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if kv.curConfig.Num > args.Num {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	for _, shard := range args.Shards {
		if kv.shardStates[shard] != Offering {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(*args)
	DPrintf("Server %v %p gid: %d (isLeader: %v) start delete shard %v index %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan OpReply, 1)
	kv.mu.Lock()
	DPrintf("Server %v %p gid: %d (isLeader: %v) start delete shard %v index %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, index)
	kv.waitChannels[int64(index)] = ch
	kv.mu.Unlock()
	select {
	case res := <-ch:
		if res.Err == OK {
			reply.Err = OK
			kv.mu.Lock()
			delete(kv.waitChannels, int64(index))
			kv.mu.Unlock()
			return
		} else {
			reply.Err = res.Err
			kv.mu.Lock()
			delete(kv.waitChannels, int64(index))
			kv.mu.Unlock()
			return
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
		return
	}
}


// =================== RPC sender ====================
func (kv *ShardKV) sendInstallShard(args InstallShardArgs) {
	DPrintf("Server %v %p gid: %d (isLeader: %v) send install shard request: num: %v, dest: %v, src: %v, shards: %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args.Dest, args.Src, args.Shards)
	for _, server := range args.Dest {
		srv := kv.make_end(server)
		var reply InstallShardReply
		ok := srv.Call("ShardKV.InstallShard", &args, &reply)
		D2Printf("Server %v %p gid: %d (isLeader: %v) send install shard request: num: %v, dest: %v server: %v, src: %v, shards: %v, reply: %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args.Dest, server, args.Src, args.Shards, reply)
		if ok && reply.Err == OK {
			return
		}
	}
}

func (kv *ShardKV) generateDeleteShardArgs(args *InstallShardArgs) DeleteShardArgs {
	arg := DeleteShardArgs {
		Num: args.Num,
		Dest: args.Src,
		Src: args.Dest,
		Shards: args.Shards,
		Keys: make([]string, 0),
	}
	for k := range args.Data {
		arg.Keys = append(arg.Keys, k)
	}
	return arg
}

// send delete shard request, use the install shard args directly
func (kv *ShardKV) sendDeleteShard(args *DeleteShardArgs) {
	D2Printf("Server %v %p gid: %d (isLeader: %v) send delete shard request: num: %v, dest: %v, src: %v, shards: %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args.Dest, args.Src, args.Shards)
	for _, server := range args.Dest {
		srv := kv.make_end(server)
		var reply DeleteShardReply
		ok := srv.Call("ShardKV.DeleteShard", args, &reply)
		if ok && reply.Err == OK {
			return
		}
	}
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
		D2Printf("Server %v %p gid: %d (isLeader: %v) receive repeated cmd from ClientId: %v sequenceNum: %v seq: %v", kv.me, kv, kv.gid, kv.isLeader(), clientId, sequenceNum, seq)
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
	e.Encode(kv.shardStates)
	e.Encode(kv.lastConfig)
	e.Encode(kv.curConfig)
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
	e_decode_shardstates := d.Decode(&kv.shardStates)
	e_decode_lastconfig := d.Decode(&kv.lastConfig)
	e_decode_curconfig := d.Decode(&kv.curConfig)
	if e_decode_clientseq != nil || e_decode_kv != nil || e_decode_shardstates != nil || e_decode_lastconfig != nil || e_decode_curconfig != nil {
		debug.PrintStack()
		log.Fatalf("decode error")
	}
}

// engine handlers
// should be in lock
func (kv *ShardKV) handleOps(op Op) OpReply {
	DPrintf("Server %v %p gid: %d (isLeader: %v) receive op: key: %s, client ID: %d, sequence num: %d, cmd type: %d", kv.me, kv, kv.gid, kv.isLeader(), op.Key, op.ClientId, op.SequenceNum, op.CmdType)
	clientId := op.ClientId
	// NOTICE: unsafe.Sizeof do not count the actual size of ptr types
	if op.SequenceNum < kv.clientSequences[clientId] {
		return OpReply{
			CmdType: op.CmdType,
			ClientId: op.ClientId,
			SequenceNum: op.SequenceNum,
			Err: OK,
			Value: kv.kv[op.Key],
		}
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
	res := OpReply {
		CmdType: op.CmdType,
		ClientId: op.ClientId,
		SequenceNum: op.SequenceNum,
		Err: OK,
		Value: kv.kv[op.Key],
	}
	DPrintf("Server %v %p gid: %d (isLeader: %v) reply: key: %s, client ID: %d, sequence num: %d, err: %s, value: %s", kv.me, kv, kv.gid, kv.isLeader(), op.Key, op.ClientId, op.SequenceNum, res.Err, res.Value)
	return res
}

// should be in lock
func (kv *ShardKV) allServing(shards []int) bool {
	for _, shard := range shards {
		if kv.shardStates[shard] != Serving {
			return false
		}
	}
	return true
}

// should be in lock
func (kv *ShardKV) handleInstallShard(args InstallShardArgs) OpReply {
	if args.Num < kv.lastConfig.Num {
		return OpReply {
			Err: OK,
		}
	}
	if args.Num > kv.curConfig.Num{
		return OpReply {
			Err: ErrShardNotReady,
		}
	}
	if kv.allServing(args.Shards) {
		return OpReply{
			Err: OK,
		}
	}
	if kv.isLeader() {
		DPrintf("Server %v %p gid: %d (isLeader: %v) install shard %v %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args)
	}
	for k, v := range args.Data {
		kv.kv[k] = v
	}
	for k, v := range args.ClientSequences {
		if v > kv.clientSequences[k] {
			kv.clientSequences[k] = v
		}
	}
	for _, shard := range args.Shards {
		kv.shardStates[shard] = Serving
	}
	return OpReply {
		Err: OK,
	}
}

// should be in lock
func (kv *ShardKV) hasServingState(shards []int) bool {
	for _, shard := range shards {
		if kv.shardStates[shard] == Serving {
			return true
		}
	}
	return false
}

// should be in lock
func (kv *ShardKV) handleDeleteShard(args DeleteShardArgs) OpReply {
	if args.Num < kv.lastConfig.Num {
		return OpReply {
			Err: OK,
		}
	}
	if args.Num > kv.curConfig.Num {
		return OpReply {
			Err: ErrShardNotReady,
		}
	}
	if kv.hasServingState(args.Shards) {
		// still need these shards
		return OpReply{
			Err: OK,
		}
	}
	if kv.isLeader() {
		DPrintf("Server %v %p gid: %d (isLeader: %v) delete shard %v %v", kv.me, kv, kv.gid, kv.isLeader(), args.Num, args)
	}
	for _, k := range args.Keys {
		delete(kv.kv, k)
	}
	for _, shard := range args.Shards {
		kv.shardStates[shard] = Serving
	}
	return OpReply {
		Err: OK,
	}
}

// should be in lock
func (kv *ShardKV) updateShardsState() {
	if kv.lastConfig.Num == 0 {
		return
	}
	for shard, gid := range kv.curConfig.Shards {
		lastGid := kv.lastConfig.Shards[shard]
		if lastGid == kv.gid && gid != kv.gid {
			kv.shardStates[shard] = Offering
		} else if lastGid != kv.gid && gid == kv.gid {
			kv.shardStates[shard] = Pulling
		}
	}

}

// should be in lock
func (kv *ShardKV) handleConfig(config shardctrler.Config) {
	if kv.isLeader() {
		D2Printf("Server %v %p gid: %d (isLeader: %v) handle config: %v", kv.me, kv, kv.gid, kv.isLeader(), config)
	}
	if config.Num == kv.curConfig.Num + 1 && kv.readyForNewSync() {
		kv.lastConfig = kv.curConfig
		kv.curConfig = config
		kv.updateShardsState()
		if kv.isLeader() {
			D2Printf("Server %v %p gid: %d (isLeader: %v) update config to %v", kv.me, kv, kv.gid, kv.isLeader(), config)
		}
	}
}

// start raft engine, handle raft messages and execute main kv operations
func (kv *ShardKV) engineStart() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			DPrintf("Server %v %p gid: %d (isLeader: %v) receive msg: %v %v", kv.me, kv, kv.gid, kv.isLeader(), msg.CommandIndex, msg.Command)
			var res OpReply
			if _, ok := msg.Command.(Op); ok {
				res = kv.handleOps(msg.Command.(Op))
			} else if _, ok := msg.Command.(InstallShardArgs); ok {
				res = kv.handleInstallShard(msg.Command.(InstallShardArgs))
			} else if _, ok := msg.Command.(DeleteShardArgs); ok {
				res = kv.handleDeleteShard(msg.Command.(DeleteShardArgs))
			} else if _, ok := msg.Command.(shardctrler.Config); ok {
				res = OpReply{
					Err: OK,
				}
				kv.handleConfig(msg.Command.(shardctrler.Config))
			}
			ch, ok := kv.waitChannels[int64(msg.CommandIndex)]
			if ok {
				ch <- res
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				D2Printf("Server %v %p gid: %d (isLeader: %v) start snapshot, raft state size: %v", kv.me, kv, kv.gid, kv.isLeader(), kv.persister.RaftStateSize())
				snapshot := kv.getSnapShot()
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

// should be in lock
func (kv *ShardKV) readyForNewSync() bool {
	for _, v := range kv.shardStates {
		if v != Serving {
			return false
		}
	}
	return true
}

func (kv *ShardKV) syncConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		if !kv.isLeader() || !kv.readyForNewSync() {
			kv.mu.Unlock()
			time.Sleep(UpdateConfigInterval)
			continue
		}
		configNum := kv.curConfig.Num
		kv.mu.Unlock()
		config := kv.mck.Query(configNum + 1)
		if config.Num > configNum {
			D2Printf("Server %v %p gid: %d (isLeader: %v) receive new config: %v %v, configNum: %v", kv.me, kv, kv.gid, kv.isLeader(), config.Num, config, configNum)
			kv.rf.Start(config)
		}
		time.Sleep(UpdateConfigInterval)
	}
}

// should be in lock
func (kv *ShardKV) needReconfigure() bool {
	for _, v := range kv.shardStates {
		if v == Offering {
			return true
		}
	}
	return false
}

func (kv *ShardKV) prepareInstallShardArgs() []InstallShardArgs {
	misplacedShards := make(map[int][]int)
	for shard, gid := range kv.curConfig.Shards {
		lastGid := kv.lastConfig.Shards[shard]
		if lastGid == kv.gid && gid != kv.gid {
			misplacedShards[gid] = append(misplacedShards[gid], shard)
		}
	}
	args := make([]InstallShardArgs, 0)
	for gid, shards := range misplacedShards {
		shardMap := make(map[int]bool)
		for _, shard := range shards {
			shardMap[shard] = true
		}
		arg := InstallShardArgs {
			Num: kv.curConfig.Num,
			Dest: kv.curConfig.Groups[gid],
			Src: kv.lastConfig.Groups[kv.gid],
			Shards: shards,
			Data: make(map[string]string),
			ClientSequences: int64TableDeepCopy(kv.clientSequences),
		}
		for k, v := range kv.kv {
			if shardMap[key2shard(k)] {
				arg.Data[k] = v
			}
		}
		args = append(args, arg)
	}
	D2Printf("Server %v %p gid: %d (isLeader: %v) prepare install shard args: %v", kv.me, kv, kv.gid, kv.isLeader(), args)
	return args
}

// send shards thread, always check if we need to send shards to others
func (kv *ShardKV) sendShards() {
	for !kv.killed() {
		kv.mu.Lock()
		if !kv.isLeader() || !kv.needReconfigure() {
			kv.mu.Unlock()
			time.Sleep(InstallShardsInterval)
			continue
		}
		argsArray := kv.prepareInstallShardArgs()
		kv.mu.Unlock()
		for _, args := range argsArray {
			kv.sendInstallShard(args)
		}
		time.Sleep(InstallShardsInterval)
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
	labgob.Register(InstallShardArgs{})
	labgob.Register(DeleteShardArgs{})
	labgob.Register(shardctrler.Config{})

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

	kv.lastConfig = shardctrler.Config{}
	kv.curConfig = shardctrler.Config{}
	kv.shardStates = make(map[int]int8)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardStates[i] = Serving
	}

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapShot(persister.ReadSnapshot())

	go kv.engineStart()
	go kv.syncConfig()
	go kv.sendShards()

	return kv
}
