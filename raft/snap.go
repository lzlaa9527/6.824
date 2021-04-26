package raft

//
// A service wants to switch to _Snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the _Snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.RWLog.mu.RLock()
	defer rf.RWLog.mu.RUnlock()

	// 不安装老旧的快照
	if lastIncludedIndex < rf.RWLog.SnapshotIndex  {
		Debug(dSnap, "[%d] S%d REFUSE INSTALL - OLD SNAP, LII:%d, SI:%d", rf.CurrentTerm, rf.me, lastIncludedIndex, rf.RWLog.SnapshotIndex)
		return false
	}
	Debug(dSnap, "[%d] S%d INSTALL SNAPSHOT, LII:%d, SI:%d", rf.CurrentTerm, rf.me, lastIncludedIndex, rf.RWLog.SnapshotIndex)
	return true
}

// the service says it has created a Snapshot that has
// all info up to and including index. this means the
// service no longer needs the RWLog through (and including)
// that index. Raft should now trim its RWLog as much as possible.
//
// 更新LastIncludeIndex、删除过时的日志、持久化日志和快照
func (rf *Raft) Snapshot(index int, snapshot []byte)  {
	// Your code here (2D).

	// 防止rf.RWLog.SnapshotIndex的读写冲突
	rf.RWLog.mu.Lock()
	Debug(dSnap, "[%d] S%d CALL SNAPSHOT, LII:%d, SI:%d", rf.CurrentTerm, rf.me, index, rf.RWLog.SnapshotIndex)

	offset := index - rf.RWLog.SnapshotIndex

	// 丢弃过时的快照
	if offset < 0 {
		Debug(dSnap, "[%d] S%d REFUSE SNAPSHOT - OLD INDEX, LII:%d, SI:%d", rf.CurrentTerm, rf.me, index, rf.RWLog.SnapshotIndex)
		rf.RWLog.mu.Unlock()
		return
	}
	rf.RWLog.SnapshotIndex = index

	// 删除原来的日志条目
	entries := make([]Entry, len(rf.Log[offset:]))
	copy(entries, rf.Log[offset:])
	rf.Log = entries

	// 首个日志条目用来存储snapshot
	rf.Log[0] = Entry{
		ApplyMsg: ApplyMsg{
			CommandValid: false,
			Command:      nil,

			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  entries[0].Term,
			SnapshotIndex: entries[0].Index,
		},
		Term:  entries[0].Term,
		Index: entries[0].Index,
	}
	rf.RWLog.mu.Unlock()

	rf.mu.RLock()
	rf.persist()
	rf.mu.RUnlock()
}
