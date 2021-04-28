package raft

import (
	"fmt"
	"sync"
)

//
// as each Raft peer becomes aware that successive RWLog entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed RWLog entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
	SnapshotValid bool
}

type Entry struct {
	ApplyMsg
	Term  int
	Index int
}


type RWLog struct {
	mu            sync.RWMutex
	Log           []Entry // 如果当前的server有快照，那么快照一定是第一个日志条目
}

func (l *RWLog) String() string {
	str := "["
	for i := 0; i < len(l.Log); i++ {
		str += fmt.Sprintf("{%d, %d},", l.Log[i].Index, l.Log[i].Term)
	}
	str += "]"
	return str
}

func (rf *Raft) applier() {
	for commitIndex := range rf.commitCh {
		if rf.killed() {
			return
		}

		// rf.lastApplied 是递增的所以不会重复执行同一个日志条目
		for commitIndex >= rf.lastApplied {
			Debug(dCommit, "[%d] S%d APPLY LA:%d", rf.CurrentTerm, rf.me, rf.lastApplied)
			rf.applyCh <- rf.Log[rf.lastApplied].ApplyMsg
			rf.lastApplied++
		}
	}
}
