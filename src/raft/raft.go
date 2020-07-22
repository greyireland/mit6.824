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

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const (
	Follower = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	UseSnapshot bool
	Snapshot    []byte
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	// 最后一条日志的任期号
	LastLogTerm int
	// 最后一条日志的索引号
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 当前任期号
	currentTerm int
	// 我投票给谁的ID
	votedFor int
	// 日志
	log []LogEntry

	// 当前待提交的日志索引
	commitIndex int
	// 上次提交的日志索引
	lastApplied int

	// Follower下一个发送的日志索引
	nextIndex []int
	// Follower和Leader匹配的日志索引
	matchIndex []int

	// 实例状态
	state int
	// 投票数
	voteCount int

	// 心跳事件
	chanHeartbeat chan bool
	// 投票事件
	chanGrantVote chan bool
	// 变更Leader事件
	chanLeader chan bool
	// 提交事件
	chanCommit chan bool
	// 提交消息
	chanApply chan ApplyMsg

	heartbeatInterval time.Duration
	// 心跳超时
	heartbeatTimeout time.Duration
	// 选举超时
	electionTimeout time.Duration
}

type LogEntry struct {
	LogIndex   int
	LogTerm    int
	LogCommand interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

func (rf *Raft) GetPersistSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}
func (rf *Raft) readSnapshot(data []byte) {
	rf.readPersist(rf.persister.ReadRaftState())
	if len(data) == 0 {
		return
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Printf("handle RequestVote,args=%+v", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	term := rf.getLastTerm()
	index := rf.getLastIndex()
	upToDate := false
	// Leader的任期号较大，日志比我新
	if args.LastLogTerm > term {
		upToDate = true
	}
	if args.LastLogTerm == term && args.LastLogIndex >= index {
		upToDate = true
	}
	// 没有投过票并且Leader日志较新
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.chanGrantVote <- true
		rf.state = Follower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Printf("handle AppendEntries,args=%+v", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // todo ？？？

	reply.Success = false
	if args.Term < rf.currentTerm {
		rf.Printf("args term < currentTerm,args=%+v,current=%+v", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	rf.Printf("receive heartbeat,reset timer")
	rf.chanHeartbeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	reply.Term = args.Term
	if args.PrevLogIndex > rf.getLastIndex() {
		rf.Printf("prevLogIndex > lastIndex")
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	baseIndex := rf.log[0].LogIndex

	if args.PrevLogIndex > baseIndex {
		term := rf.log[args.PrevLogIndex-baseIndex].LogTerm
		// 如：PrevLogTerm=5  term=6
		if args.PrevLogTerm != term {
			// term=6 后退一个term到5
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.log[i-baseIndex].LogTerm != term {
					reply.NextIndex = i + 1 // 往前退
					break
				}
			}
			return
		}
	}
	if args.PrevLogIndex < baseIndex {
		rf.Printf("should not happen")
	} else {
		rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	}
	// If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Printf("handle InstallSnapshot")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.chanHeartbeat <- true
	rf.state = Follower
	rf.persister.SaveSnapshot(args.Data)
	rf.log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)

	msg := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    args.Data,
	}

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()
	rf.chanApply <- msg
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []LogEntry) []LogEntry {
	var newLog []LogEntry
	newLog = append(newLog, LogEntry{
		LogIndex: lastIncludedIndex,
		LogTerm:  lastIncludedTerm,
	})
	// 取snapshot之后的日志即可
	for i := len(log) - 1; i >= 0; i-- {
		if log[i].LogIndex == lastIncludedIndex && log[i].LogTerm == lastIncludedTerm {
			newLog = append(newLog, log[i+1:]...)
			break
		}
	}
	return newLog
}

//
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
//
func (rf *Raft) sendRequestVoteRPC(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.state == Leader {
		isLeader = true
		index = rf.getLastIndex() + 1
		rf.log = append(rf.log, LogEntry{
			LogIndex:   index,
			LogTerm:    term,
			LogCommand: command,
		})
		rf.persist()
	}
	rf.Printf("start command index: %+v,term: %+v,isLeader: %+v,", index, term, isLeader)
	return index, term, isLeader
}
func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].LogIndex
	lastIndex := rf.getLastIndex()
	if index <= baseIndex || index > lastIndex {
		return
	}
	var newLog []LogEntry
	newLog = append(newLog, LogEntry{
		LogIndex: index,
		LogTerm:  rf.log[index-baseIndex].LogTerm,
	})
	for i := index + 1; i <= lastIndex; i++ {
		newLog = append(newLog, rf.log[i-baseIndex])
	}
	rf.log = newLog

	rf.persist()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLog[0].LogIndex)
	e.Encode(newLog[0].LogTerm)
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}
func (rf *Raft) election() {
	rf.Printf("start init")
	for {
		switch rf.state {
		case Follower:
			rf.Printf("current state=Follower")
			// 心跳事件或者给他人投票，重置计时器
			select {
			case <-rf.chanHeartbeat:
				rf.Printf("receive heartbeat")
			case <-rf.chanGrantVote:
				rf.Printf("grant vote")
			case <-time.After(rf.heartbeatTimeout):
				rf.Printf("heartbeat timeout,change state to candidate")
				rf.state = Candidate
			}
		case Candidate:
			rf.Printf("current state=Candidate")
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			go rf.broadcastRequestVote()
			select {
			case <-time.After(rf.electionTimeout): // election again
				rf.Printf("election timeout,do it again")
			case <-rf.chanHeartbeat:
				rf.Printf("election state receive heartbeat")
				rf.state = Follower
			case <-rf.chanLeader:
				rf.Printf("candidate change to leader")
				rf.mu.Lock()
				rf.state = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = rf.getLastIndex() + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			}
		case Leader:
			rf.Printf("current state=Leader")
			rf.broadcastAppendEntries()
			time.Sleep(rf.heartbeatInterval)
		}
	}
}
func (rf *Raft) broadcastRequestVote() {
	rf.Printf("start broadcastRequestVote")
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate {
			go rf.sendRequestVote(i, args)
		}
	}
}
func (rf *Raft) sendRequestVote(i int, args RequestVoteArgs) {
	rf.Printf("%+v sendRequestVote to %+v,args=%+v", rf.me, i, args)
	var reply RequestVoteReply
	ok := rf.sendRequestVoteRPC(i, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Printf("receive sendRequestVote result,args=%+v,reply=%+v", args, reply)
	if ok {
		term := rf.currentTerm
		if rf.state != Candidate {
			return
		}
		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == Candidate && rf.voteCount > len(rf.peers)/2 {
				rf.Printf("sendRequestVote voteCount>n/2,change state to leader")
				rf.state = Follower // 改变状态，防止多次发送选主成功数事件
				rf.chanLeader <- true
			}
		}
	}
}
func (rf *Raft) broadcastAppendEntries() {
	rf.Printf("start broadcastAppendEntries")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitLog()
	baseIndex := rf.log[0].LogIndex
	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {

			if rf.nextIndex[i] > baseIndex {
				rf.Printf("start appendEntries to peer: %+v,nextIndex: %+v,baseIndex: %+v", i, rf.nextIndex[i], baseIndex)
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].LogTerm
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1-baseIndex:]))
				copy(args.Entries, rf.log[args.PrevLogIndex+1-baseIndex:])
				args.LeaderCommit = rf.commitIndex
				go rf.sendAppendEntries(i, args)
			} else {
				rf.Printf("start installSnapshot to peer: %+v,nextIndex: %+v,baseIndex: %+v", i, rf.nextIndex[i], baseIndex)
				var args InstallSnapshotArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.log[0].LogIndex
				args.LastIncludedTerm = rf.log[0].LogTerm
				args.Data = rf.persister.ReadSnapshot()
				go rf.sendInstallSnapshot(i, args)
			}
		}
	}
}
func (rf *Raft) commitLog() {
	rf.Printf("start commitLog")
	commitIndex := rf.commitIndex
	last := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i &&
				rf.log[i-baseIndex].LogTerm == rf.currentTerm {
				num++
			}
		}
		if num > len(rf.peers)/2 {
			commitIndex = i
		}
	}
	if commitIndex > rf.commitIndex {
		rf.Printf("commit log index: %+v", commitIndex)
		rf.commitIndex = commitIndex
		rf.chanCommit <- true // 提交日志
	}
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {
	rf.Printf("start sendAppendEntries")
	var reply AppendEntriesReply
	ok := rf.sendAppendEntriesRPC(server, &args, &reply)
	rf.Printf("receive AppendEntries reply=%+v", reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != Leader {
			rf.Printf("state changed,current state=%+v", rf.state)
			return
		}
		if args.Term != rf.currentTerm {
			rf.Printf("term changed,args=%+v", args)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.Printf("reply term > current term")
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return
		}
		if reply.Success {
			rf.Printf("sendAppendEntries success,reply=%+v", reply)
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.Printf("sendAppendEntries failed,reply=%+v", reply)
			rf.nextIndex[server] = reply.NextIndex
		}
		//todo 提交日志
		//rf.commitLog()
	}
}
func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs) {
	rf.Printf("start sendInstallSnapshot")
	var reply InstallSnapshotReply
	ok := rf.sendInstallSnapshotRPC(server, &args, &reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			return
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
}

func (rf *Raft) applyLog() {
	for {
		select {
		case <-rf.chanCommit:
			rf.Printf("start applyLog")
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			baseIndex := rf.log[0].LogIndex
			for i := rf.lastApplied + 1; i <= commitIndex; i++ {
				msg := ApplyMsg{CommandIndex: i, Command: rf.log[i-baseIndex].LogCommand, CommandValid: true}
				rf.Printf("applyLog %+v", msg)
				rf.chanApply <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

//
// the service or tester wants to create a Raft server. the ports
// of all the  Raftservers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.state = Follower
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0, LogIndex: 0})
	rf.currentTerm = 0
	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanApply = applyCh
	rf.heartbeatInterval = heartbeatInterval()
	rf.heartbeatTimeout = heartbeatTimeout()
	rf.electionTimeout = electionTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go rf.election()
	go rf.applyLog()
	return rf
}

func (rf *Raft) Printf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("%+v server: %d,state: %d,term: %d, ",
		time.Now().Format("2006-01-02 15:04:05.000"), rf.me, rf.state, rf.currentTerm)
	content := fmt.Sprintf(format, a...)
	fmt.Println(prefix + content)

}
