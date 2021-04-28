package raft

//
// A service wants to switch to _Snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the _Snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a Snapshot that has
// all info up to and including index. this means the
// service no longer needs the RWLog through (and including)
// that index. Raft should now trim its RWLog as much as possible.
//
// 更新LastIncludeIndex、删除过时的日志、持久化日志和快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {

}
