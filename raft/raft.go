package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// =================================================
// Raft State
// =================================================

type NodeState int

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state NodeState
	log   []LogEntry

	lastResetTime time.Time

	applyCh chan raftapi.ApplyMsg

	currentTerm int
	votedFor    int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastIncludedIndex int
	lastIncludedTerm  int

	electionTimeout time.Duration

	snapshotPending raftapi.ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int = rf.currentTerm
	var isleader bool = rf.state == Leader

	return term, isleader
}

func (rf *Raft) getCurrIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) toPhysical(logicalIdx int) int {
	return logicalIdx - rf.lastIncludedIndex
}

func (rf *Raft) toLogical(physicalIdx int) int {
	return physicalIdx + rf.lastIncludedIndex
}

// =================================================
// State
// =================================================

func (rf *Raft) convertToFollower() {
	rf.votedFor = -1
	rf.state = Follower
	rf.persist(rf.persister.ReadSnapshot())
}

func (rf *Raft) stepDown(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.state = Follower
	rf.persist(rf.persister.ReadSnapshot())
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist(rf.persister.ReadSnapshot())
	rf.resetElectionTimeout()
	go rf.callElection()
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	for peer := range rf.peers {
		rf.nextIndex[peer] = rf.getCurrIndex() + 1
		rf.matchIndex[peer] = 0
	}
	go rf.heartbeatTicker()
}

// =================================================
// Log
// =================================================

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// =================================================
// APPEND ENTRIES
// =================================================
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// heart beat -- new Leader
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	// reject appendEntry --  stale Leader
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.stepDown(args.Term)
	}

	rf.resetElectionTimeout()

	prevPhysIdx := rf.toPhysical(args.PrevLogIndex)

	if prevPhysIdx < 0 {
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	// reject appendEntry -- conflict index or term
	if prevPhysIdx >= len(rf.log) || args.PrevLogTerm != rf.log[prevPhysIdx].Term {
		if prevPhysIdx >= len(rf.log) {
			reply.ConflictIndex = rf.toLogical(len(rf.log))
		} else {
			conflictTerm := rf.log[prevPhysIdx].Term
			i := prevPhysIdx
			for i > 0 && rf.log[i-1].Term == conflictTerm {
				i--
			}
			reply.ConflictIndex = rf.toLogical(i)
		}
		reply.Success = false
		return
	}

	i := 0
	for ; i < len(args.Entries); i++ {
		physIdx := prevPhysIdx + 1 + i
		if physIdx >= len(rf.log) {
			break
		}
		if rf.log[physIdx].Term != args.Entries[i].Term {
			break
		}
	}

	// truncate and apply leader's log entries
	if i < len(args.Entries) {
		rf.log = rf.log[:prevPhysIdx+1+i]
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist(rf.persister.ReadSnapshot())
	}

	// commitIndex must never point past the (possibly just-truncated) log
	if rf.commitIndex > rf.getCurrIndex() {
		rf.commitIndex = rf.getCurrIndex()
	}

	// partioned leader check
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getCurrIndex())
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	return ok
}

// =================================================
// Agreement
// =================================================
func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		return
	}
	match := make([]int, len(rf.peers))
	copy(match, rf.matchIndex)
	match[rf.me] = rf.getCurrIndex()
	sort.Ints(match)
	n := match[len(match)/2]

	if n > rf.commitIndex && rf.log[rf.toPhysical(n)].Term == rf.currentTerm {
		// fmt.Printf("[gid?] me=%d term=%d commitIndex=%d\n", rf.me, rf.currentTerm, rf.commitIndex)
		rf.commitIndex = n
	}
}

// =================================================
// Apply Commited Logs
// =================================================
func (rf *Raft) applierTicker() {
	for {
		rf.mu.Lock()
		if rf.snapshotPending.SnapshotValid {
			rf.mu.Unlock()
			rf.applyCh <- rf.snapshotPending
			rf.mu.Lock()
			rf.snapshotPending = raftapi.ApplyMsg{}
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.toPhysical(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			if msg.Command != nil {
				rf.applyCh <- msg
			}
			rf.mu.Lock()
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// =================================================
// Heart Beat
// =================================================

func (rf *Raft) heartbeatTicker() {
	for true {
		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.broadcastHeartbeats()
		rf.mu.Unlock()
	}
}

// Empty Append Entries for Heart Beat
func (rf *Raft) broadcastHeartbeats() {
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		if rf.nextIndex[peer] <= rf.lastIncludedIndex {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}

			go func(peer int, args *InstallSnapshotArgs) {
				reply := InstallSnapshotReply{}
				if ok := rf.sendInstallSnapshot(peer, args, &reply); ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertToFollower()
						return
					}

					if rf.currentTerm != args.Term || rf.state != Leader {
						return
					}

					if args.LastIncludedIndex > rf.matchIndex[peer] {
						rf.matchIndex[peer] = args.LastIncludedIndex
						rf.nextIndex[peer] = args.LastIncludedIndex + 1
					}
					rf.updateCommitIndex()
				}
			}(peer, &args)

			continue
		}

		prevLogIndex := rf.nextIndex[peer] - 1
		prevLogTerm := rf.log[rf.toPhysical(prevLogIndex)].Term

		entries := make([]LogEntry, len(rf.log[rf.toPhysical(rf.nextIndex[peer]):]))
		copy(entries, rf.log[rf.toPhysical(rf.nextIndex[peer]):])

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func(peer int, args *AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(peer, args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.convertToFollower()
					return
				}

				if rf.currentTerm != args.Term || rf.state != Leader {
					return
				}

				if reply.Success {
					newNext := args.PrevLogIndex + len(args.Entries) + 1
					newMatch := newNext - 1
					if newMatch > rf.matchIndex[peer] {
						rf.matchIndex[peer] = newMatch
					}
					if newNext > rf.nextIndex[peer] {
						rf.nextIndex[peer] = newNext
					}
					rf.updateCommitIndex()
				} else {
					if reply.ConflictIndex > 0 && reply.ConflictIndex < rf.nextIndex[peer] {
						rf.nextIndex[peer] = reply.ConflictIndex
					}
				}
			}
		}(peer, &args)
	}
}

// =================================================
// Request Vote
// =================================================
type RequestVoteArgs struct {
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	lastLogIndex := rf.getCurrIndex()
	lastLogTerm := rf.log[rf.toPhysical(lastLogIndex)].Term

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	reply.Term = rf.currentTerm

	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}

	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist(rf.persister.ReadSnapshot())
	reply.VoteGranted = true
	rf.resetElectionTimeout()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// =================================================
// Election
// =================================================

func (rf *Raft) callElection() {
	rf.mu.Lock()

	electionTerm := rf.currentTerm
	lastLogIndex := rf.getCurrIndex()
	lastLogTerm := rf.log[rf.toPhysical(lastLogIndex)].Term

	args := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	votesReceived := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.convertToFollower()
				return
			}

			if rf.currentTerm != electionTerm || rf.state != Candidate {
				return
			}

			if reply.VoteGranted && rf.state == Candidate {
				votesReceived++

				if votesReceived > len(rf.peers)/2 {
					rf.convertToLeader()
				}
			}
		}(peer)
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.lastResetTime = time.Now()
	rf.electionTimeout = time.Duration(150+(rand.Int63()%450)) * time.Millisecond
}

func (rf *Raft) ticker() {
	for true {
		ms := 50 + (rand.Int63() % 300)
		randomTimeout := time.Duration(ms) * time.Millisecond
		time.Sleep(randomTimeout)

		rf.mu.Lock()
		if time.Since(rf.lastResetTime) >= rf.electionTimeout {
			if rf.state != Leader {
				rf.convertToCandidate()
			}
		}
		rf.mu.Unlock()
	}
}

// =================================================
// Snapshot
// =================================================

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

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastIncludedIndex+len(rf.log)-1 {
		return
	}

	physIdx := rf.toPhysical(index)

	rf.lastIncludedTerm = rf.log[physIdx].Term
	rf.log = rf.log[physIdx:]
	rf.lastIncludedIndex = index
	rf.persist(snapshot)
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.stepDown(args.Term)
	}

	rf.resetElectionTimeout()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	physIdx := rf.toPhysical(args.LastIncludedIndex)
	if physIdx < len(rf.log) && physIdx >= 0 && args.LastIncludedTerm == rf.log[physIdx].Term {
		rf.log = rf.log[physIdx:]
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.persist(args.Data)

	rf.snapshotPending = raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", &args, &reply)
	return ok
}

// =================================================
// Persist
// =================================================

func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("failed to decode persisted state")
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// =================================================
// Start
// =================================================

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state == Leader

	if !isLeader {
		return -1, -1, false
	}

	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   rf.getCurrIndex() + 1,
	}
	rf.log = append(rf.log, logEntry)
	rf.persist(rf.persister.ReadSnapshot())
	rf.broadcastHeartbeats()
	index := logEntry.Index
	return index, rf.currentTerm, true
}

// =================================================
// Establish
// =================================================

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	// COMMUNICATION
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// ELECTION
	rf.state = Follower
	rf.resetElectionTimeout()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	// NEXT & MATCH INDEX
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := range len(peers) {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// LOG
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil, Index: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	snapshot := rf.persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex

		rf.snapshotPending = raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applierTicker()

	return rf
}

func (rf *Raft) reached(peer int) {
	fmt.Printf("\n%d reached ==========================\n", peer)
}
