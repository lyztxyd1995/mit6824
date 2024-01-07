package raft

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func commitLogTask(rf *Raft) {
	sleepDuration := time.Duration(100) * time.Millisecond
	for {
		if isKilled(rf) {
			fmt.Println("Id: " + strconv.Itoa(rf.me) + " has been kiiled, stop the commit log task")
			// set to killed state
			atomic.StoreInt32(&rf.state, 3)
			return
		}
		if atomic.LoadInt32(&rf.state) == 0 {
			rf.commitLog()
			time.Sleep(sleepDuration)
		} else {
			fmt.Println("Id: " + strconv.Itoa(rf.me) + " is no longer leader, stop the commit log task")
			return
		}
	}
}

func (rf *Raft) commitLog() {
	commitChannel := make(chan bool)
	rf.logMu.Lock()
	logLength := len(rf.logs)
	rf.logMu.Unlock()
	commitIndex := int(atomic.LoadInt32(&rf.commitedIdx))
	currentTerm := int(atomic.LoadInt32(&rf.term))
	if commitIndex == logLength-1 {
		// logs are all commited, just send heartbeat message
		appendEntryArgs := &AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			Entries:      make([]Log, 0),
			LeaderCommit: commitIndex,
		}
		appendEntryReply := &AppendEntriesReply{}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(peerId int) {
					rf.sendAppendEntries(peerId, appendEntryArgs, appendEntryReply)
				}(i)
			}
		}
		return
	}
	nextCommitIndex := commitIndex + 1
	fmt.Println("id: " + strconv.Itoa(rf.me) + " start to commit log for index " + strconv.Itoa(nextCommitIndex))

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peerId int) {
				prevLogIndex := rf.nextIndex[peerId] - 1
				var prevLogTerm int
				if prevLogIndex == -1 {
					prevLogTerm = 0
				} else {
					prevLogTerm = rf.logs[prevLogIndex].Term
				}
				appendEntryArgs := &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      rf.logs[prevLogIndex+1 : nextCommitIndex+1],
					LeaderCommit: int(rf.commitedIdx),
				}
				appendEntryReply := &AppendEntriesReply{}
				succeed := false
				for !succeed {
					fmt.Println("id: " + strconv.Itoa(rf.me) + " send log to " + strconv.Itoa(peerId) + " with PrevLogIndex: " + strconv.Itoa(appendEntryArgs.PrevLogIndex))
					fmt.Println(appendEntryArgs.Entries)

					succeed = rf.sendAppendEntries(peerId, appendEntryArgs, appendEntryReply)
					if succeed {
						if appendEntryReply.Success {
							rf.nextIndex[peerId] = nextCommitIndex + 1
							fmt.Println("next index for id " + strconv.Itoa(peerId) + " is updated to " + strconv.Itoa(nextCommitIndex+1))
							commitChannel <- appendEntryReply.Success
						} else {
							fmt.Println("id: " + strconv.Itoa(rf.me) + " commit not success")
							if appendEntryReply.Term > currentTerm {
								// the follower's current term is greater than the leader's term, return false
								commitChannel <- false
							} else {
								// decrements the prev index and re-send request
								appendEntryArgs.PrevLogIndex--
								rf.nextIndex[peerId] = appendEntryArgs.PrevLogIndex + 1
								if appendEntryArgs.PrevLogIndex >= 0 {
									appendEntryArgs.Entries = rf.logs[appendEntryArgs.PrevLogIndex : nextCommitIndex+1]
								} else {
									appendEntryArgs.Entries = rf.logs[:nextCommitIndex+1]
								}
								succeed = false
							}
						}
					}
				}
			}(i)
		}
	}
	sleepDuration := time.Duration(20) * time.Millisecond
	numOfPeers := len(rf.peers) - 1
	numOfReply := 0
	numOfAccept := 0
	for {
		select {
		case appendLogRes := <-commitChannel:
			numOfReply++
			if appendLogRes {
				numOfAccept++
				if numOfAccept >= numOfPeers/2 {
					// commit the new log
					atomic.StoreInt32(&rf.commitedIdx, int32(nextCommitIndex))
					fmt.Println("log index " + strconv.Itoa(nextCommitIndex) + " is commited")
					fmt.Println(rf.logs)
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[nextCommitIndex].Command,
						CommandIndex: nextCommitIndex + 1,
					}
					rf.applyCh <- applyMsg
					return
				}
			}
			if numOfReply == numOfPeers {
				// Fail to commit the log, just return
				return
			}
		default:
			time.Sleep(sleepDuration)
		}
	}
}
