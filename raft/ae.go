package raft

import (
	"sync/atomic"
	"unsafe"
)

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int // leader's CurrentTerm
	LeaderId     int // so follower can redirect clients
	LeaderCommit int // leader's commitIndex

	PrevLogIndex int // index of Log entry immediately preceding new ones
	PrevLogTerm  int // CurrentTerm of prevLogIndex entry

	// For snapshot
	LastIncludeTerm  int
	LastIncludeIndex int
	Log              []Entry
	Snapshot         bool // 是否包含Snapshot
}

type AppendEntriesReply struct {
	// Your data here (2A).

	Valid bool

	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 加锁保证了image各字段的一致性
	rf.mu.RLock()
	image := *rf.Image
	rf.mu.RUnlock()

	var (
		me          = rf.me
		term        = args.Term
		currentTerm = image.CurrentTerm
		leaderID    = args.LeaderId
	)
	reply.Term = max(term, currentTerm) // 响应的term一定是较大值

	Debug(dAppend, "[%d] S%d RECEIVE<- S%d AE,T:%d PLI:%d PLT:%d LEN:%d", currentTerm, me, leaderID, term, args.PrevLogIndex, args.PrevLogTerm, len(args.Log))

	if term < currentTerm {
		reply.Success = false
		Debug(dAppend, "[%d] S%d REFUSE <- S%d, LOWER TERM.", currentTerm, me, leaderID)
		reply.Valid = true
		return
	}

	// 收到leader的AE RPC，需要转化为FOLLOWER并重置自己的计时器
	reply.Valid = image.Update(func(i *Image) {
		i.State = FOLLOWER
		i.CurrentTerm = term
		i.VotedFor = leaderID

		// 如果当前的server一直是该Leader的Follower就不需要改变状态，
		// 否则就应该改变状态并使得之前的Image实例失效。
		if !(term == currentTerm && image.State == FOLLOWER) {
			close(i.done)              // 使旧的Image实例失效
			i.done = make(chan signal) // 使新的Image实例生效
		}
		// 这里需要同步更新Image实例，因为下面还要进行日志一致性检查
		image = *i
		currentTerm = image.CurrentTerm
		Debug(dAppend, "[%d] S%d CONVERT FOLLOWER <- S%d.", term, me, leaderID)
		i.resetTimer() // 重置计时器
	})

	// server的状态已经发生了改变
	if !reply.Valid {
		return
	}

	// LEADER发来不含快照的日志
	if !args.Snapshot {

		// 避免对rf.Log的读写冲突，这里并不能保证server的状态不会发生改变
		rf.RWLog.mu.RLock()
		var (
			prevLogIndex = args.PrevLogIndex
			prevLogTerm  = args.PrevLogTerm
		)
		// LEADER日志条目在FOLLOWER日志中的偏移位置
		prevLogIndex -= rf.RWLog.SnapshotIndex

		// 不处理超时RPC中过时的日志
		if prevLogIndex < 0 {
			rf.RWLog.mu.RUnlock()
			Debug(dAppend, "[%d] S%d REFUSE <- S%d, OLD LOG", currentTerm, me, leaderID)
			reply.Valid = false
			return
		}

		// 日志不匹配
		if prevLogIndex >= len(rf.Log) {
			reply.ConflictIndex = len(rf.Log)
			reply.ConflictTerm = -1
			reply.Success = false

			Debug(dAppend, "[%d] S%d REFUSE <- S%d, CONFLICT", currentTerm, me, leaderID)
			rf.RWLog.mu.RUnlock()

			reply.Valid = !image.Done()
			return
		}

		// 若RWLog[prevLogIndex].Term != prevLogTerm，需要找到ConflictIndex然后返回false
		if prevLogTerm != rf.Log[prevLogIndex].Term {
			reply.Success = false
			reply.ConflictTerm = rf.Log[prevLogIndex].Term

			// 找到首个term为ConflictTerm的ConflictIndex
			for reply.ConflictIndex = prevLogIndex; rf.Log[reply.ConflictIndex].Term == reply.ConflictTerm; reply.ConflictIndex-- {
			}

			reply.ConflictIndex++
			// ConflictIndex的绝对索引
			reply.ConflictIndex += rf.RWLog.SnapshotIndex

			rf.RWLog.mu.RUnlock()
			Debug(dAppend, "[%d] S%d REFUSE <- S%d, CONFLICT", currentTerm, me, leaderID)

			// 如果server状态已经改变，上述数据是无效的
			reply.Valid = !image.Done()
			return
		}
		rf.RWLog.mu.RUnlock()
	}

	// 通过了日志的一致性检查，开始追加日志
	reply.Success = true

	// 要保证追加日志时server的状态没有发生改变，所以这里加读锁
	// 如果不加锁server状态可能发生改变，有可能删除其它leader添加的已提交的日志
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	reply.Valid = !image.Done()
	// 如果Image实例已经失效，即刻退出
	if image.Done() {
		return
	}

	// 添加日志后，可能需要更新commitIndex
	// 这里不需要加锁，因为只要不减小commitIndex都是安全的; (减小commitIndex，可能让一条日志条目提交两次)
	defer func() {

		var (
			leaderCommit   = args.LeaderCommit
			newCommitIndex int
		)

		if !args.Snapshot {
			newCommitIndex = min(leaderCommit, args.PrevLogIndex+len(args.Log))
		} else {
			newCommitIndex = min(leaderCommit, args.LastIncludeIndex+len(args.Log)-1)
		}

		// 判断commitIndex是否需要更新
		var commitIndex int
		commitIndex = int(atomic.LoadInt64((*int64)(unsafe.Pointer(&rf.commitIndex))))

		// 因为申请了读锁，server的状态不会发生改变
		// 只有在commitIndex < newCommitIndex时才更新保证了commitIndex的单调性，也保证了正确性
		// 使用CAS是为了保证并发安全，否则可能覆盖别的协程提交的修改
		if commitIndex < newCommitIndex && atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&rf.commitIndex)), int64(commitIndex), int64(newCommitIndex)) {

			// 一边去提交就好了
			go func() {
				// 通知commit协程提交日志
				Debug(dCommit, "[%d] S%d UPDATE  CI:%d <-S%d", currentTerm, me, newCommitIndex, leaderID)
				rf.commitCh <- newCommitIndex
			}()
		}
	}()

	// 响应AE RPC之前要将添加的日志持久化
	defer func() {
		rf.persist()
	}()

	// 为了保证并发修改日志的正确性，这里申请日志写锁
	rf.RWLog.mu.Lock()
	defer rf.RWLog.mu.Unlock()

	// i，j作为指针扫描FOLLOWER与LEADER的日志，检查是否存在冲突日志
	i, j := args.PrevLogIndex-rf.RWLog.SnapshotIndex+1, 0 // AE RPC中不含快照

	// 处理快照
	if args.Snapshot {

		lastIncludeIndex := args.LastIncludeIndex - rf.RWLog.SnapshotIndex

		// 虽然LEADER的快照过时了但是随着快照发来的可能有新的日志条目应该被添加
		// LEADER不改变状态的前提下，日志添加操作是幂等的，所以不会出错
		if lastIncludeIndex < 0 {
			// 从-lastIncludeIndex处扫描LEADER发来的日志，检查是否有可添加的日志
			i, j = 0, -lastIncludeIndex
		} else {
			if lastIncludeIndex >= len(rf.Log) {
				// FOLLOWER丢弃掉过时的所有日志
				rf.Log = args.Log
			} else {
				// FOLLOWER压缩日志丢掉部分日志
				log := make([]Entry, len(rf.Log[lastIncludeIndex:])) // 避免内存泄露
				copy(log, rf.Log[lastIncludeIndex:])
				rf.Log = log
			}

			// 接受了快照之后更新snapshotIndex；snapshotIndex是单调增加的
			rf.RWLog.SnapshotIndex = args.LastIncludeIndex
			i, j = 0, 0
		}
	}

	for ; i < len(rf.Log) && j < len(args.Log); i, j = i+1, j+1 {

		// 日志term不匹配，或者term匹配但是一个是快照，一个不是也不行
		if rf.Log[i].Term != args.Log[j].Term || rf.Log[i].CommandValid == args.Log[j].SnapshotValid {
			break
		}
	}

	// 日志添加完成
	if j >= len(args.Log) {
		Debug(dAppend, "[%d] S%d APPEND <- S%d, LEN:%d", currentTerm, me, leaderID, len(args.Log))
		return
	}

	// 出现冲突日志，丢弃掉后续日志
	if i < len(rf.Log) {
		log := make([]Entry, len(rf.Log[:i]))
		copy(log, rf.Log[:i])
		rf.Log = log
		Debug(dAppend, "[%d] S%d DROP <- S%d, CLI:%d", currentTerm, me, leaderID, i)
	}

	// 追加剩余日志
	rf.Log = append(rf.Log, args.Log[j:]...)

	Debug(dAppend, "[%d] S%d APPEND <- S%d, LEN:%d", currentTerm, me, leaderID, len(args.Log))
}

// Aerpc 向标号为peerIndex的peer发送AE RPC，并处理后续响应
// image		发送RPC时server的Image实例
// nextIndex	发送RPC时peer的nextIndex值
// matchIndex	发送RPC时peer的matchIndex值
// args			RPC参数
func aerpc(image Image, peerIndex int, nextIndex, matchIndex int, args *AppendEntriesArgs) {

	reply := new(AppendEntriesReply)
	image.peers[peerIndex].Call("Raft.AppendEntries", args, reply)

	// 无效的响应，或者server的状态已经发生改变，就放弃当前的任务
	if image.Done() || !reply.Valid {
		return
	}
	Debug(dAppend, "[%d] S%d AE <-REPLY S%d, V:%v S:%v CLI:%d, CLT:%d", image.CurrentTerm, image.me, peerIndex, reply.Valid, reply.Success, reply.ConflictIndex, reply.ConflictTerm)

	// server应该转为FOLLOWER，但不用重置计时器
	if reply.Term > image.CurrentTerm {
		image.Update(func(i *Image) {

			// 设置新的Image
			i.State = FOLLOWER
			i.CurrentTerm = reply.Term
			i.VotedFor = -1
			Debug(dTimer, "[%d] S%d CONVERT FOLLOWER <- S%d NEW TERM.", i.CurrentTerm, i.me, peerIndex)

			// 使旧的Image实例失效
			close(i.done)
			// 使新Image实例生效
			i.done = make(chan signal)
		})
		return
	}

	var (
		// 更新matchIndex、nextIndex
		newNextIndex  = nextIndex
		newMatchIndex = matchIndex
	)

	// 日志匹配不成功
	if !reply.Success {
		image.RWLog.mu.RLock()
		snapshotIndex := image.RWLog.SnapshotIndex
		// nextIndex不能小于snapshotIndex，因为snapshot之前的日志已经被删除了
		newNextIndex = max(reply.ConflictIndex, snapshotIndex)
		// 如果reply.ConflictTerm != -1从日志中搜索和FOLLOWER可以匹配的日志条目
		if reply.ConflictTerm != -1 {

			// 如果image.Log[newNextIndex - snapshotIndex].Term != reply.ConflictTerm，
			// 那么newNextIndex左边的日志也不可能与FOLLOWER的日志匹配
			if image.Log[newNextIndex-snapshotIndex].Term == reply.ConflictTerm {
				for ; newNextIndex < args.PrevLogIndex && image.Log[newNextIndex-snapshotIndex].Term == reply.ConflictTerm; newNextIndex++ {
				}
			}
		}
		image.RWLog.mu.RUnlock()
	} else {
		// 日志匹配的情况
		newNextIndex += len(args.Log)
		newMatchIndex = newNextIndex - 1
	}

	// 不需要更新nextIndex，matchIndex
	if nextIndex == newNextIndex && matchIndex == newMatchIndex {
		return
	}

	// 如果server的状态没有改变，更新peer的nextIndex、newMatchIndex值
	// 采用CAS的方式，保证并发情况下nextIndex、matchIndex的正确性
	// 即使server的状态已经发生改变不再是leader，修改nextIndex、matchIndex也是无害的
	if !image.Done() && atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&image.nextIndex[peerIndex])), int64(nextIndex), int64(newNextIndex)) &&
		atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&image.matchIndex[peerIndex])), int64(matchIndex), int64(newMatchIndex)) {
		Debug(dAppend, "[%d] S%d UPDATE M&N -> S%d, MI:%d, NI:%d", image.CurrentTerm, image.me, peerIndex, newMatchIndex, newNextIndex)
	}
}

// CalculateCommitIndex 在leader发送心跳之前计算commitIndex
func (rf *Raft) calculateCommitIndex() {

	// 采用CAS的思想，更新commitIndex，如果在提交之前commitIndex发生改变，就放弃提交；
	// 因此整个过程就没必要加锁了。
	// oldCommitIndex := int(atomic.LoadInt64((*int64)(unsafe.Pointer(&rf.commitIndex))))
	// oldCommitIndex := rf.commitIndex
	newCommitIndex := -1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 计算matchIndex的上限，加速计算出正确的commitIndex
		// 对于matchIndex的读操作可能存在读写冲突，但是不影响正确性
		newCommitIndex = max(rf.matchIndex[i], newCommitIndex)
	}

	// 找到首个能提交的日志条目，更新commitIndex
	for {
		count := 0
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= newCommitIndex {
				count++
			}
		}
		// 找到首个大多数server均接受的日志条目时就不用再向前找了
		if count >= len(rf.peers)/2 {
			break
		}
		newCommitIndex--
	}

	// if newCommitIndex < rf.lastApplied {
	// 	return
	// }

	// 没必要更新commitIndex字段
	if newCommitIndex <= rf.commitIndex {
		return
	}

	// 不能提交其它任期的entry
	rf.RWLog.mu.RLock()
	if rf.Log[newCommitIndex-rf.RWLog.SnapshotIndex].Term != rf.CurrentTerm {
		rf.RWLog.mu.RUnlock()
		return
	}
	rf.RWLog.mu.RUnlock()

	rf.commitIndex = newCommitIndex
	// 通知applier协程提交日志，放在协程内执行可以让calculateCommitIndex尽快返回
	go func() {
		Debug(dCommit, "[%d] S%d UPDATE CI:%d", rf.CurrentTerm, rf.me, newCommitIndex)
		rf.commitCh <- newCommitIndex
	}()

}

func (rf *Raft) SendAppendEntries() {
	image := *rf.Image
	rf.calculateCommitIndex() // 更新commitIndex
	Debug(dAppend, "[%d] S%d SEND AE RPC.", rf.CurrentTerm, rf.me)

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:         image.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}
		// 记录下此时的nextIndex,matchIndex；
		// 在收到RPC响应之后如果nextIndex,matchIndex发生改变，无需更新nextIndex,matchIndex
		var (
			nextIndex  = rf.nextIndex[server]
			matchIndex = rf.matchIndex[server]
		)

		rf.RWLog.mu.RLock()

		snapshotIndex := rf.RWLog.SnapshotIndex
		// 当nextIndex<=snapshotIndex时发送快照，
		if nextIndex <= snapshotIndex {
			nextIndex = snapshotIndex
			rf.nextIndex[server] = nextIndex // 别忘了更新nextIndex

			args.Snapshot = true
			lastIncludeIndex := snapshotIndex
			lastIncludeTerm := rf.Log[0].Term // 快照的下标是0

			args.LastIncludeIndex = lastIncludeIndex
			args.LastIncludeTerm = lastIncludeTerm
			args.Log = append(rf.Log[:0:0], rf.Log[:]...)
		} else {
			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.Log[prevLogIndex-snapshotIndex].Term

			args.PrevLogIndex = prevLogIndex
			args.PrevLogTerm = prevLogTerm
			args.Log = append(rf.Log[:0:0], rf.Log[nextIndex-snapshotIndex:]...)
		}

		rf.RWLog.mu.RUnlock()
		go aerpc(image, server, nextIndex, matchIndex, args)
	}
}
