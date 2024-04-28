package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)


type Op struct {
	// Your data here.
	CmdType int8
	ClientId int64
	SequenceNum int64

	// Join
	Servers map[int][]string

	// Leave
	GIDs []int

	// Move
	Shard int
	GID int

	// Query
	Num int
}

type OpReply struct {
	// Your data here.
	CmdType int8
	ClientId int64
	SequenceNum int64
	WrongLeader bool
	Err string
	Config Config
}


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	clientSequences map[int64]int64
	waitChannels map[int]chan OpReply

	configs []Config // indexed by config num
}

// for faster leader recognization
func (sc *ShardCtrler) GetState(args *GetStateArgs, reply *GetStateReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, isLeader := sc.rf.GetState()
	reply.IsLeader = isLeader
}


// common action for Get, Put, Append, wait for engine extract raft's output and manage with kv storage
func (sc *ShardCtrler) waitCmd(cmd Op, index int) OpReply {
	sc.mu.Lock()
	ch := make(chan OpReply, 1)
	sc.waitChannels[index] = ch
	sc.mu.Unlock()
	
	select {
	case res := <-ch:
		if res.CmdType != cmd.CmdType || res.ClientId != cmd.ClientId || res.SequenceNum != cmd.SequenceNum  {
			res.Err = ErrCmd
		}
		sc.mu.Lock()
		delete(sc.waitChannels, index)
		sc.mu.Unlock()
		return res
	case <-time.After(100 * time.Millisecond):
		sc.mu.Lock()
		delete(sc.waitChannels, index)
		sc.mu.Unlock()
		res := OpReply{
			Err: ErrTimeout,
		}
		return res
	}
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if args.SequenceNum <= sc.clientSequences[args.ClientId] {
		sc.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Unlock()
	cmd := Op{
		CmdType: JoinCmd,
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		Servers: args.Servers,
	}
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	res := sc.waitCmd(cmd, index)
	reply.WrongLeader = res.WrongLeader
	reply.Err = Err(res.Err)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.SequenceNum <= sc.clientSequences[args.ClientId] {
		sc.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Unlock()
	cmd := Op{
		CmdType: LeaveCmd,
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		GIDs: args.GIDs,
	}
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	res := sc.waitCmd(cmd, index)
	reply.WrongLeader = res.WrongLeader
	reply.Err = Err(res.Err)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.SequenceNum <= sc.clientSequences[args.ClientId] {
		sc.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Unlock()
	cmd := Op{
		CmdType: MoveCmd,
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		Shard: args.Shard,
		GID: args.GID,
	}
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	res := sc.waitCmd(cmd, index)
	reply.WrongLeader = res.WrongLeader
	reply.Err = Err(res.Err)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if args.SequenceNum <= sc.clientSequences[args.ClientId] {
		sc.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Unlock()
	cmd := Op{
		CmdType: QueryCmd,
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		Num: args.Num,
	}
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	res := sc.waitCmd(cmd, index)
	reply.WrongLeader = res.WrongLeader
	reply.Err = Err(res.Err)
	sc.mu.Lock()
	if !reply.WrongLeader && reply.Err == OK {
		reply.Config = res.Config.DeepCopy()
	}
	sc.mu.Unlock()
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// rebalance the shards, make sure each group has the almost the same number of shards
// should be in lock
func (sc *ShardCtrler) balance() {
	config := sc.configs[len(sc.configs) - 1]
	if len(config.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}
	// gids: sorted GIDs
	gids := make([]int, 0)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	// gidshards: gid -> shards, a group has multiple shards, every gid has a list, including gid 0 for tmp storage
	gidshards := make(map[int][]int)
	for _, gid := range gids {
		gidshards[gid] = make([]int, 0)
	}
	if gids[0] != 0 {
		gidshards[0] = make([]int, 0)
	}
	for i := 0; i < NShards; i++ {
		gidshards[config.Shards[i]] = append(gidshards[config.Shards[i]], i)
	}
	// numGroups: number of groups, numShards: number of shards, avg: average number of shards per group, all groups should have avg or avg + 1 shards, and the groups with avg + 1 shards should be no more than remains
	numGroups := len(gids)
	numShards := NShards
	avg := numShards / numGroups
	remains := numShards % numGroups
	DPrintf("Server %v balance: numGroups: %v, numShards: %v, avg: %v, remains: %v, gids: %v, gidshards: %v", sc.me, numGroups, numShards, avg, remains, gids, gidshards)
	// balance algorithm: we need scan twice, first scan to make sure all groups have no more than avg or avg + 1 shards, second scan to move the gid 0's extra shards to groups with less than avg shards
	// first scan
	for i := 0; i < numGroups; i++ {
		if gids[i] == 0 {
			continue
		}
		if remains > 0 {
			// we allow these groups have avg + 1 shards
			if len(gidshards[gids[i]]) > avg + 1 {
				for j := avg + 1; j < len(gidshards[gids[i]]); j++ {
					gidshards[0] = append(gidshards[0], gidshards[gids[i]][j])
					config.Shards[gidshards[gids[i]][j]] = 0
				}
				remains--
			}
		} else {
			// we allow these groups have avg shards
			if len(gidshards[gids[i]]) > avg {
				for j := avg; j < len(gidshards[gids[i]]); j++ {
					gidshards[0] = append(gidshards[0], gidshards[gids[i]][j])
					config.Shards[gidshards[gids[i]][j]] = 0
				}
			}
		}
	}
	DPrintf("Server %v first scan gids: %v, gidshards: %v", sc.me, gids, gidshards)
	// second scan
	for i := 0; i < numGroups; i++ {
		if gids[i] == 0 {
			continue
		}
		if len(gidshards[0]) == 0 {
			break
		}
		if len(gidshards[gids[i]]) < avg {
			for j := len(gidshards[gids[i]]); j < avg && len(gidshards[0]) > 0; j++ {
				config.Shards[gidshards[0][0]] = gids[i]
				gidshards[gids[i]] = append(gidshards[gids[i]], gidshards[0][0])
				gidshards[0] = gidshards[0][1:]
			}
		}
	}
	DPrintf("Server %v second scan gids: %v, gidshards: %v", sc.me, gids, gidshards)
	sc.configs[len(sc.configs) - 1] = config
}


func (sc *ShardCtrler) executeJoin(cmd Op) {
	newConfig := sc.configs[len(sc.configs) - 1].DeepCopy()
	newConfig.Num++
	for gid, servers := range cmd.Servers {
		newConfig.Groups[gid] = servers
	}
	sc.configs = append(sc.configs, newConfig)
	DPrintf("Server %v execute join cmd: %v, before: config: %v", sc.me, cmd, sc.configs[len(sc.configs) - 1])
	sc.balance()
	DPrintf("Server %v execute join cmd: %v, after: config: %v", sc.me, cmd, sc.configs[len(sc.configs) - 1])
}

func (sc *ShardCtrler) executeLeave(cmd Op) {
	newConfig := sc.configs[len(sc.configs) - 1].DeepCopy()
	newConfig.Num++
	for _, gid := range cmd.GIDs {
		delete(newConfig.Groups, gid)
	}
	for i := 0; i < NShards; i++ {
		if _, ok := newConfig.Groups[newConfig.Shards[i]]; !ok {
			newConfig.Shards[i] = 0
		}
	}
	sc.configs = append(sc.configs, newConfig)
	sc.balance()
}

func (sc *ShardCtrler) executeMove(cmd Op) {
	newConfig := sc.configs[len(sc.configs) - 1].DeepCopy()
	newConfig.Num++
	newConfig.Shards[cmd.Shard] = cmd.GID
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) isRepeated(clientId, seqNo int64) bool {
	value, ok := sc.clientSequences[clientId]
	if ok && value >= seqNo {
		return true
	}
	return false
}

func (sc *ShardCtrler) engineStart() {
	for {
		msg := <-sc.applyCh
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			if cmd.SequenceNum < sc.clientSequences[cmd.ClientId] {
				continue
			}
			sc.mu.Lock()
			if !sc.isRepeated(cmd.ClientId, cmd.SequenceNum) {
				if cmd.CmdType == JoinCmd {
					sc.executeJoin(cmd)
				} else if cmd.CmdType == LeaveCmd {
					sc.executeLeave(cmd)
				} else if cmd.CmdType == MoveCmd {
					sc.executeMove(cmd)
				}
			}
			DPrintf("Server %v receive cmd: %v, clientSequences: %v", sc.me, cmd, cmd.SequenceNum)
			if cmd.SequenceNum > sc.clientSequences[cmd.ClientId] {
				sc.clientSequences[cmd.ClientId] = cmd.SequenceNum
			}
			if ch, ok := sc.waitChannels[msg.CommandIndex]; ok {
				reply := OpReply{
					CmdType: cmd.CmdType,
					ClientId: cmd.ClientId,
					SequenceNum: cmd.SequenceNum,
					WrongLeader: false,
					Err: OK,
				}
				if cmd.CmdType == QueryCmd {
					if cmd.Num < 0 || cmd.Num >= len(sc.configs) {
						reply.Config = sc.configs[len(sc.configs) - 1].DeepCopy()
					} else {
						reply.Config = sc.configs[cmd.Num].DeepCopy()
					}
				}
				ch <- reply
			}
			sc.mu.Unlock()
		}
	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientSequences = make(map[int64]int64)
	sc.waitChannels = make(map[int]chan OpReply)

	go sc.engineStart()

	return sc
}
