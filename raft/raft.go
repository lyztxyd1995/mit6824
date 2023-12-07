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
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// ApplyMsg struct.
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Raft is a Go object implementing a single Raft peer.

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	term     int32
	votedFor map[int]int
	state    int32 // 0: Leader 1: follower 2: Candidate

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(atomic.LoadInt32(&rf.term))
	state := atomic.LoadInt32(&rf.state)
	isleader = state == 0

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

// RequestVoteArgs represents an example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// RequestVoteReply is an example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	VotedFor    int
}

// RequestVote is an example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if isKilled(rf) {
		reply.VoteGranted = false
		fmt.Println("Id: " + strconv.Itoa(rf.me) + " has been kiiled, deny the vote")
		return
	}
	fmt.Println("Id: " + strconv.Itoa(rf.me) + " receive vote request")
	fmt.Println(args)

	term := int(atomic.LoadInt32(&rf.term))
	// Reject vote if term is less or equal with the current term
	if term > args.Term {
		reply.VoteGranted = false
		reply.Term = term
		return
	}

	rf.mu.Lock()
	votedForId, exists := rf.votedFor[args.Term]
	rf.mu.Unlock()
	if !exists || votedForId == args.CandidateId {
		reply.VoteGranted = true
		reply.Term = args.Term
		fmt.Println("Id: " + strconv.Itoa(rf.me) + " grant the vote")
	} else {
		reply.VoteGranted = false
		reply.Term = args.Term
		fmt.Println("Id: " + strconv.Itoa(rf.me) + " deny the vote with voted for " + strconv.Itoa(int(votedForId)))
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if isKilled(rf) {
		fmt.Println("Id: " + strconv.Itoa(rf.me) + " has been kiiled, deny the append entry request")
		return
	}
	term := int(atomic.LoadInt32(&rf.term))
	if term < args.Term {
		atomic.StoreInt32(&rf.term, int32(args.Term))
		atomic.StoreInt32(&rf.state, 1)
		fmt.Println(strconv.Itoa(rf.me) + " update term to: " + strconv.Itoa(args.Term))
	} else if term == args.Term {
		atomic.StoreInt32(&rf.state, 1)
	} else {
		fmt.Println(strconv.Itoa(rf.me) + " receive heartbeat from legacy leader with term: " + strconv.Itoa(args.Term))
	}
}

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

// Start func.
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill func.
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
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func sendAppendEntiresTask(rf *Raft) {
	duration := time.Duration(100) * time.Millisecond
	for {
		if isKilled(rf) {
			fmt.Println("Id: " + strconv.Itoa(rf.me) + " has been kiiled, stop the heartbeat")
			// set to killed state
			atomic.StoreInt32(&rf.state, 3)
			return
		}
		if atomic.LoadInt32(&rf.state) == 0 {
			term := int(atomic.LoadInt32(&rf.term))
			appendEntryArgs := &AppendEntriesArgs{Term: term, LeaderId: rf.me}
			appendEntryReply := &AppendEntriesReply{}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(peerId int) {
						// fmt.Println("Id: " + rf.id + " send append entry request to " + strconv.Itoa(peerId))
						rf.sendAppendEntries(peerId, appendEntryArgs, appendEntryReply)
					}(i)
				}
			}
		} else {
			// not leader anymore, end the hearbeat
			break
		}
		time.Sleep(duration)
	}
}

func isKilled(rf *Raft) bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func electionTimeoutTask(rf *Raft) {
	duration := 200 + rand.Intn(100)
	randomDuration := time.Duration(duration) * time.Millisecond
	for {
		if isKilled(rf) {
			fmt.Println("Id: " + strconv.Itoa(rf.me) + " has been kiiled, stop the election check")
			return
		}
		time.Sleep(randomDuration)
		state := int(atomic.LoadInt32(&rf.state))
		if state == 2 {
			// increase term
			term := int(atomic.AddInt32(&rf.term, 1))
			rf.mu.Lock()
			_, exists := rf.votedFor[term]
			if !exists {
				fmt.Println("Id: " + strconv.Itoa(rf.me) + " trys to select for leader with term: " + strconv.Itoa(term))
				rf.votedFor[term] = rf.me
				rf.mu.Unlock()
				numberOfPeers := len(rf.peers) - 1
				numberOfVoteGranted := 0
				var mu sync.Mutex // to safely update numberOfVoteGranted
				var wg sync.WaitGroup
				requestVoteArgs := &RequestVoteArgs{Term: term, CandidateId: rf.me}
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						wg.Add(1)
						go func(peerId int) {
							defer wg.Done()
							requestVoteReply := &RequestVoteReply{}
							fmt.Println("Id: " + strconv.Itoa(rf.me) + " sends vote request to: " + strconv.Itoa(peerId) + " with new term: " + strconv.Itoa(term))
							succeed := rf.sendRequestVote(peerId, requestVoteArgs, requestVoteReply)
							if !succeed {
								fmt.Println("vote request failed " + strconv.Itoa(peerId) + ": " + "Id: " + strconv.Itoa(rf.me) + " sends vote request to: " + strconv.Itoa(peerId))
								mu.Lock()
								numberOfPeers--
								mu.Unlock()
							} else if requestVoteReply.VoteGranted {
								mu.Lock()
								numberOfVoteGranted++
								mu.Unlock()
							}
						}(i)
					}
				}
				wg.Wait()
				fmt.Println("Id: " + strconv.Itoa(rf.me) + " receive " + strconv.Itoa(numberOfVoteGranted) + " votes out of : " + strconv.Itoa(numberOfPeers))
				if numberOfVoteGranted > 0 && numberOfVoteGranted >= numberOfPeers/2 {
					// set the state as leader
					atomic.StoreInt32(&rf.state, 0)
					fmt.Println("Id: " + strconv.Itoa(rf.me) + " becomes the leader for term " + strconv.Itoa(term))
					go sendAppendEntiresTask(rf)
				}
			}
		} else if state == 1 {
			// update from Follower state to Candidate state
			atomic.StoreInt32(&rf.state, 2)
		}
	}
}

// Make func.
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	fmt.Println("Create raft with id: " + strconv.Itoa(me))
	atomic.StoreInt32(&rf.term, 0)
	atomic.StoreInt32(&rf.state, 2)
	atomic.StoreInt32(&rf.dead, 0)
	rf.votedFor = make(map[int]int)

	// Your initialization code here (2A, 2B, 2C).
	// background process to for leader election
	go electionTimeoutTask(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
