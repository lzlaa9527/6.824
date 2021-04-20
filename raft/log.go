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
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	ApplyMsg
	Term int
}

type RWLog struct {
	mu  sync.RWMutex
	Log []Entry
}

func (l *RWLog) String() string {
	str := "["
	for i := 0; i < len(l.Log); i++ {
		command := l.Log[i].Command
		var s = command
		switch command.(type) {
		case int:
			break
		case string:

			n := len(command.(string))
			if n <= 5 {
				s = command
			} else {
				s = command.(string)[:3] + "..." + command.(string)[n-2:]
			}
		}
		str += fmt.Sprintf("{%d, %d, %v},", l.Log[i].CommandIndex, l.Log[i].Term, s)
	}
	str += "]"
	return str
}

func (rf *Raft) commit() {

	for commitIndex := range rf.commitCh {
		if rf.killed() {
			return
		}

		// 在提交日志之前进行持久化，持久化过程中要避免server状态的转化
		rf.mu.RLock()
		rf.persist()
		rf.mu.RUnlock()

		Debug(dCommit, "[*] S%d Commit LA:%d, CI:%d", rf.me, rf.lastApplied, commitIndex)
		for commitIndex >= rf.lastApplied {
			rf.Log[rf.lastApplied].ApplyMsg.CommandValid = true // 提交的时候一定要设置CommandValid = true
			rf.applyCh <- rf.Log[rf.lastApplied].ApplyMsg

			rf.lastApplied++
		}
	}

}
