package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// ========================== Imports ============================

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// ========================== Structs ============================
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	leader State = iota
	follower State = iota
	candidate State = iota
)

type LogEntry struct {
	Index int
	Term int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Server state
	state State

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log		 []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex []int
	matchIndex []int

	// Channel
	applyCh chan ApplyMsg
	applyCond *sync.Cond

	// timers
	lastHeartbeat time.Time
	
}

// ========================== Lib Function ============================

func (rf *Raft) resetTimer() {
	rf.lastHeartbeat = time.Now()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.Unlock()
	return term, isleader
}

// should be in lock
func (rf *Raft) getFirstLogIndex() int {
	// log always have 1 item, check no need
	return rf.log[0].Index
}

// should be in lock
func (rf *Raft) getFirstLogTerm() int {
	// log always have 1 item, check no need
	return rf.log[0].Term
}

// should be in lock
func (rf *Raft) getLastLogIndex() int {
	// log always have 1 item, check no need
	return rf.log[len(rf.log) - 1].Index
}

// should be in lock
func (rf *Raft) getLastLogTerm() int {
	// log always have 1 item, check no need
	return rf.log[len(rf.log) - 1].Term
}

// should be in lock
func (rf *Raft) updateCommitIndex() bool {
	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N - rf.getFirstLogIndex()].Term == rf.currentTerm; N-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		DPrintf("Server %v (Term: %v) count: %v, N: %v len(rf.peers): %v", rf.me, rf.currentTerm, count, N, len(rf.peers))
		if count > len(rf.peers) / 2 {
			rf.commitIndex = N
			DPrintf("Server %v (Term: %v) commitIndex: %v", rf.me, rf.currentTerm, rf.commitIndex)
			rf.applyCond.Signal()
			return true
		}
	}
	return false
}

// should be in lock
func (rf *Raft) isLogUpToDate(index int, term int) bool {
	lastLogIndex, lastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	DPrintf("Server %v (Term: %v) isLogUpToDate: %v, %v, %v, %v", rf.me, rf.currentTerm, index, term, lastLogIndex, lastLogTerm)
	// Section 5.4
	return term > lastLogTerm || (term == lastLogTerm && index >= lastLogIndex)
}

// ========================== RPC responsers ============================

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTermRd int
	var votedForRd int
	var logRd []LogEntry
	if d.Decode(&currentTermRd) != nil || d.Decode(&votedForRd) != nil || d.Decode(&logRd) != nil {
		log.Fatalf("Server %v (Term: %v) readPersist error", rf.me, rf.currentTerm)
	} else {
		rf.currentTerm = currentTermRd
		rf.votedFor = votedForRd
		rf.log = logRd
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %v (Term: %v) received RequestVote from %v, rf.votedFor: %v", rf.me, rf.currentTerm, args, rf.votedFor)
	if rf.state == leader && rf.currentTerm >= args.Term {
		// if we are leader, we should not vote for others
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if (args.Term < rf.currentTerm) || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId && rf.votedFor != rf.me) {
		// if request is old or we have voted for other candidate
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	} else if args.Term > rf.currentTerm && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// if we are outdated
		rf.currentTerm, rf.votedFor = args.Term, -1
	} else if rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) == false {
		// if log is not up to date
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		DPrintf("Server %v (Term: %v) refuse voted for %v", rf.me, rf.currentTerm, args.CandidateId)
		return
	}
	// grant the vote
	DPrintf("Server %v (Term: %v) voted for %v", rf.me, rf.currentTerm, args.CandidateId)
	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.stateChange(follower)
	rf.persist()
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	FirstIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetTimer()
	DPrintf("Server %v (Term: %v) received AppendEntries from %v", rf.me, rf.currentTerm, args.LeaderId)
	if args.Term < rf.currentTerm {
		// if request is old
		reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, rf.getLastLogIndex() + 1
		return
	}
	if args.Term > rf.currentTerm {
		// if we are outdated
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.stateChange(follower)

	if args.PrevLogIndex > rf.getLastLogIndex() {
		// if log doesn't match
		reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, rf.getLastLogIndex() + 1
		return
	}

	if args.PrevLogIndex < rf.getFirstLogIndex() - 1 {
		log.Fatalf("Invalid PrevLogIndex: %v, FirstLogIndex: %v", args.PrevLogIndex, rf.getFirstLogIndex())
	}

	match_term := rf.log[args.PrevLogIndex - rf.getFirstLogIndex()].Term
	if args.PrevLogTerm != match_term {
		// if log doesn't match
		for i := args.PrevLogIndex - 1; i >= rf.getFirstLogIndex(); i-- {
			if rf.log[i - rf.getFirstLogIndex()].Term != match_term {
				reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, false, i + 1
				return
			}
		}
	}

	rf.log = rf.log[:args.PrevLogIndex - rf.getFirstLogIndex() + 1]
	rf.log = append(rf.log, args.Entries...)
	reply.Term, reply.Success, reply.FirstIndex = rf.currentTerm, true, args.PrevLogIndex + len(args.Entries)

	DPrintf("Server %v (Term: %v) commitIndex: %v leadercommit: %v getLastLogIndex: %v", rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit, rf.getLastLogIndex())
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Signal()
	}
	rf.persist()
}

// ========================== RPC Callers ============================

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// shall be called within lock: at least we can promise that the state is not changed until all goroutines launchedd
// broadcast requestvote to all peers
func (rf *Raft) broadcastRequestVote() {
	DPrintf("Server %v (Term: %v) broadcast RequestVote", rf.me, rf.currentTerm)
	if rf.state != candidate {
		fmt.Printf("Invalid state for broadcastRequestVote: %v\n", rf.state)
		return
	}
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm: rf.getLastLogTerm(),
	}
	// votes counter, including itself
	receivedVotes := 1
	for i := range rf.peers {
		if i != rf.me {
			// send to all other peers
			go func (peer int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != candidate {
						return
					}
					if rf.currentTerm == args.Term {
						// make sure request valid
						if reply.VoteGranted {
							// someone vote us
							receivedVotes++
							DPrintf("Server %v (Term: %v) received vote from %v, receivedVotes: %v", rf.me, rf.currentTerm, peer, receivedVotes)
							if receivedVotes > len(rf.peers) / 2 {
								rf.stateChange(leader)
							}
						} else {
							// someone newer than us
							if reply.Term > rf.currentTerm {
								rf.currentTerm, rf.votedFor = reply.Term, -1
								rf.stateChange(follower)
								rf.persist()
							}
						}
					}
				}
			} (i)
		}
	}
}

// shall be called within lock: at least we can promise that the state is not changed until all goroutines launched
// broadcast heartbeat to all peers
// broadcast heartbeat also do send append entry work
func (rf *Raft) broadcastHeartBeat() {
	DPrintf("Server %v (Term: %v) broadcast HeartBeat", rf.me, rf.currentTerm)
	if rf.state != leader {
		fmt.Printf("Server %v (Term: %v) Invalid state for broadcastHeartBeat: %v\n", rf.me, rf.currentTerm, rf.state)
		return
	}
	for i := range rf.peers {
		if i != rf.me {
			go func (peer int) {
				reply := &AppendEntriesReply{}
				rf.mu.Lock()
				args := &AppendEntriesArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
				}
				DPrintf("Server %v (Term: %v) getLastLogIndex: %v nextIndex: %v\n", rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.nextIndex[peer])
				args.PrevLogIndex = rf.nextIndex[peer] - 1
				args.PrevLogTerm = rf.log[rf.nextIndex[peer] - 1].Term
				args.Entries = rf.log[rf.nextIndex[peer] - rf.getFirstLogIndex():]
				args.LeaderCommit = rf.commitIndex
				DPrintf("Server %v (Term: %v) send AppendEntries to %v, args: %v", rf.me, rf.currentTerm, peer, args)
				rf.mu.Unlock()
				if rf.sendAppendEntries(peer, args, reply) {
					rf.mu.Lock()
					if rf.state == leader && args.Term == rf.currentTerm {
						if reply.Term > rf.currentTerm {
							rf.currentTerm, rf.votedFor = reply.Term, -1
							rf.stateChange(follower)
							rf.persist()
						} else {
							if reply.Success {
								if len(args.Entries) > 0 {
									rf.nextIndex[peer] = args.Entries[len(args.Entries) - 1].Index + 1
									rf.matchIndex[peer] = rf.nextIndex[peer] - 1
									DPrintf("Server %v (Term: %v) nextIndex: %v matchIndex: %v", rf.me, rf.currentTerm, rf.nextIndex[peer], rf.matchIndex[peer])
								}
								rf.updateCommitIndex()
							} else {
								rf.nextIndex[peer] = min(reply.FirstIndex, rf.getLastLogIndex())
							}
						}
					}
					rf.mu.Unlock()
				}
			} (i)
		}
	
	}
}

// ========================== Start ============================

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %v (Term: %v, State: %v) Start: %v", rf.me, rf.currentTerm, rf.state, command)
	if rf.state != leader {
		isLeader = false
	} else {
		// append log
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.persist()
	}

	return index, term, isLeader
}

// ========================== Extra ============================

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// shall be called within lock
func (rf *Raft) stateChange(newState State) {
	rf.resetTimer()
	DPrintf("Server %v (Term: %v) state change from %v to %v", rf.me, rf.currentTerm, rf.state, newState)
	if rf.state == newState {
		// no change
		return
	}
	if newState == leader {
		// if we become leader
		if rf.state != candidate {
			log.Fatalf("Server %v (Term %v) Invalid state change to leader from %v", rf.me, rf.currentTerm, rf.state)
		}
		rf.state = leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		newIndex := len(rf.log)
		for i := range rf.peers {
			rf.nextIndex[i] = newIndex
			rf.matchIndex[i] = 0
		}
	} else if newState == candidate {
		// if we become candidate
		if rf.state != follower {
			log.Fatalf("Server %v (Term %v) Invalid state change to candidate from %v", rf.me, rf.currentTerm, rf.state)
		}
		rf.state = candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
	} else {
		// if we become follower
		rf.state = follower
		rf.votedFor = -1
		rf.persist()
	}
}

// ========================== go routines ============================

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == follower {
			// if the server is follower
			if time.Now().Sub(rf.lastHeartbeat) > time.Duration(500 + (rand.Int63() % 300)) * time.Millisecond {
				// we shall detect the time gap since we receive leader's last heartbeat, too long means the leader is dead, we should start an election
				DPrintf("Server %v (Term: %v) is now a candidate, duration: %v", rf.me, rf.currentTerm, time.Now().Sub(rf.lastHeartbeat))
				rf.stateChange(candidate)
				rf.resetTimer()
				rf.broadcastRequestVote()
			}
		} else if rf.state == leader {
			// if the server is leader, broadcast heartbeat time by time
			rf.broadcastHeartBeat()
		} else {
			if time.Now().Sub(rf.lastHeartbeat) > time.Duration(500 + (rand.Int63() % 300)) * time.Millisecond {
				// Way 1:
				// if the server is candidate but disconnected, when it come back it should return to normal state
				// successfully
				// rf.stateChange(follower)
				// rf.resetTimer()
				// Way 2:
				// if the server is candidate but disconnected, when it come back it should continue the election
				rf.resetTimer()
				rf.broadcastRequestVote()
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + rand.Int63() % 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.lastApplied < rf.commitIndex {
			DPrintf("Server %v (Term: %v) applyLog: %v, %v, %v", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.log)
			msgs := make([]ApplyMsg, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command: rf.log[i - rf.getFirstLogIndex()].Command,
					CommandIndex: i,
				})
			}
			commitIndex := rf.commitIndex
			rf.mu.Unlock()
			for _, msg := range msgs {
				rf.applyCh <- msg
			}
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, commitIndex)
		}
		rf.mu.Unlock()
	}
}

// ========================== Main ============================

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]LogEntry, 0)
	// we use a bubble log to simplify the initial case, since the requests have a prev index argument
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0, Command: nil})

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.peers {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}
	
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLog()

	return rf
}
