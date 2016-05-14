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

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"
import "fmt"
import "strconv"
import "math/rand"
import "time"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type RaftState int

const (
	Follower RaftState = iota
	Leader
	Candidate
)

type LogEntry struct {
	Index int
	Term int	
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// persistent
	currentTerm int
	votedFor int
	logs []LogEntry
	
	// volatile
	commitIndex int
	lastApplied int
	state RaftState
	leaderID int
	
	rand *rand.Rand
	
	// election
	electionTimeout int
	electionTimer *time.Timer
	votes map[int]bool
	votesLock *sync.Mutex
	
	//heartbeat
	heartbeatTimeout int
	heartbeatTimer *time.Timer
	
	// leader only
	nextIndex []int
	matchIndex []int
	heartbeatSendTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	
	fmt.Printf("%d is leader %v term %v\n", rf.me, 
		rf.state == Leader ,rf.currentTerm)
	
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	ID int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	
	if(args.Term < rf.currentTerm) {
		return
	}
	
	if(args.Term > rf.currentTerm) {
		rf.becomeFollower(args.Term)	
	}
	
	// fmt.Printf("%v voted for is %v\n", rf.me, rf.votedFor)
	if(rf.votedFor == -1 || rf.votedFor == args.ID) {
		// if(args.LastLogIndex >= rf.commitIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.ID
		// }
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int) {
	
	var args RequestVoteArgs
	var reply RequestVoteReply
	
	args.Term = rf.currentTerm
	args.ID = rf.me
	args.LastLogIndex = rf.lastApplied
	args.LastLogTerm = rf.currentTerm
	
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	
	if(!ok) {
		fmt.Printf("failed to sendRequestVote to %v\n", server)	
		return
	}
	
	// state changed to follower
	if(rf.state != Candidate) {
		return
	}
	
	rf.votesLock.Lock();
	rf.votes[server] = reply.VoteGranted
	rf.votesLock.Unlock();
	
	// fmt.Printf("%d receives from %d vote %v\n", 
	//	rf.me, server, reply.VoteGranted);
	
	count := rf.votedCount()
	fmt.Printf("%d got %d votes\n", rf.me, count)
	
	majority := len(rf.peers)/2+1
		
	switch(majority) {
		case count:
			rf.becomeLeader()
		case len(rf.votes) - count :
			rf.becomeFollower(rf.currentTerm)
	}
}

type AppendEntriesArgs struct {
	// Your data here.
	Term int
	LeaderID int
	PrevLogIndex int
	Entries []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here.
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.

	reply.Term = rf.currentTerm
	reply.Success = false
	
	// fmt.Printf("append entries curr %v lead %v\n", rf.me, args.Term)
	
	rf.heartbeatTimer.Reset(time.Millisecond * 
		time.Duration(rf.heartbeatTimeout))
		
	if(args.Term < rf.currentTerm) {
		return
	}
	
	if(rf.currentTerm < args.Term) {
		rf.becomeFollower(args.Term)
	}

	rf.leaderID = args.LeaderID
	
	return 
}

func (rf *Raft) sendAppendEntries() {
	for i:=0;i<len(rf.peers);i++ {
		if(i == rf.me) {
			continue	
		}
		
		go func(server int) {
			var args AppendEntriesArgs
			var reply AppendEntriesReply
			
			// fmt.Printf("term in append %v\n", rf.currentTerm)
			args.Term = rf.currentTerm
			args.LeaderID = rf.me
			
			rf.peers[server].Call("Raft.AppendEntries", args, &reply)	
			
		}(i);
	}
	
	rf.heartbeatSendTimer.Reset(
		time.Millisecond*
		time.Duration(rf.heartbeatTimeout/2))
	
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// for peer in range rf.peers {
		// peer.Call("Raft.AppendEntries", &args, &reply)	
	// }
	
	return index, term, rf.state == Leader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) reset(term int) {
	if(rf.currentTerm != term) {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	
	timeout := 200
	rf.electionTimeout = 2*(timeout + rf.rand.Intn(timeout))
	rf.heartbeatTimeout = timeout + rf.rand.Intn(timeout)
	
	if(rf.heartbeatTimer == nil) {
		rf.heartbeatTimer = time.NewTimer(
			time.Millisecond * 
			time.Duration(rf.heartbeatTimeout))
	}
	
	if(rf.electionTimer == nil){	
		rf.electionTimer = time.NewTimer(
			time.Millisecond * 
			time.Duration(rf.electionTimeout))
	}
	
	if(rf.heartbeatSendTimer == nil) {
		rf.heartbeatSendTimer = 
			time.NewTimer(time.Millisecond*
            time.Duration(rf.heartbeatTimeout/2))
	}
	
	rf.heartbeatTimer.Stop()
	rf.electionTimer.Stop()
	rf.heartbeatSendTimer.Stop();
	
	rf.votes = make(map[int]bool)
}

func (rf *Raft) becomeLeader() {
	fmt.Printf("%d become leader at term %d\n", rf.me, rf.currentTerm)
	
	rf.reset(rf.currentTerm)
	rf.state = Leader
	
	go func ()  {
		for {
			
			fmt.Printf("send HB \n")
			go rf.sendAppendEntries()
			<- rf.heartbeatSendTimer.C
		}	
	}()
}

func (rf *Raft) becomeCandidate() {
	rf.reset(rf.currentTerm + 1)
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.votes = make(map[int]bool)
	fmt.Printf("%d become candidate at term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeFollower(term int) {
	fmt.Printf("%d become follower at term %d\n", rf.me, rf.currentTerm)
	
	rf.reset(term)
	rf.state = Follower

	rf.heartbeatTimer.Reset(time.Millisecond * 
		time.Duration(rf.heartbeatTimeout))	
	
	go func() {		
		<-rf.heartbeatTimer.C
		
		rf.heartbeatTimer.Stop()
		rf.compaign()
	}()
}

func (rf *Raft) votedCount() int {
	count := 0
	
	for _, granted := range rf.votes {
		if(granted == true) {
			count ++		
		}
	}	
	
	return count
}

func (rf *Raft) compaign() {
	rf.becomeCandidate()
	
	rf.votes[rf.me] = true
	
	rf.electionTimer.Reset(
		time.Millisecond * 
		time.Duration(rf.electionTimeout))
	
	for i:=0;i<len(rf.peers);i++ {

		if(i == rf.me) {
			continue
		}
			
		go rf.sendRequestVote(i);
	}
	
	go func(){
		<-rf.electionTimer.C
		if(rf.state == Candidate) {
			rf.compaign()
		}
	}()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
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
	
	// Your initialization code here.
	rf.votedFor = -1
	rf.rand = rand.New(rand.NewSource(int64(me)))
	rf.votesLock = &sync.Mutex{}
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Println("Make " + strconv.Itoa(me))
	rf.becomeFollower(rf.currentTerm)
	return rf
}
