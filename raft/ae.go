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
	HasSnapshot      bool // 是否包含Snapshot
}

type AppendEntriesReply struct {
	// Your data here (2A).

	Valid bool

	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries 处理Leader的AE RPC请求
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

	Debug(dAppend, "[%d] R%d RECEIVE<- R%d AE,T:%d HAS:%v PLI:%d PLT:%d, LLI:%d, LLT:%d LEN:%d", currentTerm, me, leaderID, term, args.HasSnapshot, args.PrevLogIndex, args.PrevLogTerm, args.LastIncludeIndex, args.LastIncludeTerm, len(args.Log))

	if term < currentTerm {
		reply.Success = false
		Debug(dAppend, "[%d] R%d REFUSE <- R%d, LOWER TERM.", currentTerm, me, leaderID)
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
		Debug(dAppend, "[%d] R%d CONVERT FOLLOWER <- R%d.", term, me, leaderID)
		i.resetTimer() // 重置计时器
	})

	// server的状态已经发生了改变
	if !reply.Valid {
		return
	}

	// 这里加读锁保证追加日志时server的状态没有发生改变
	// 如果不加锁server状态可能发生改变，有可能删除其它leader添加的已提交的日志
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	reply.Valid = !image.Done()
	// 如果Image实例已经失效，即刻退出
	if image.Done() {
		return
	}

	// 避免对rf.Log的读写冲突
	rf.RWLog.mu.Lock()
	defer rf.RWLog.mu.Unlock()

	// i，j作为指针扫描FOLLOWER与LEADER的日志，检查是否存在冲突日志
	var i, j int

	// 一致性检查
	if !args.HasSnapshot { // 不含快照的一致性检查

		var (
			prevLogIndex = args.PrevLogIndex
			prevLogTerm  = args.PrevLogTerm
		)
		// LEADER日志条目在FOLLOWER日志中的偏移位置
		prevLogIndex -= rf.RWLog.SnapshotIndex

		if prevLogIndex < 0 { // FOLLOWER日志中不存在prevLogIndex执行的日志条目
			offset := -prevLogIndex - 1  // FOLLOWER日志的第一个条目在RPC日志中的位置
			if offset >= len(args.Log) { // RPC中所有的日志都是过时的
				Debug(dAppend, "[%d] R%d REFUSE <- R%d, OLD LOG", currentTerm, me, leaderID)
				reply.Success = true
				return
			}

			// RPC中可能存在FOLLOWER没有的日志
			i, j = 1, offset+1
		} else {

			// 日志不匹配
			if prevLogIndex >= len(rf.Log) {
				reply.ConflictIndex = len(rf.Log)
				reply.ConflictTerm = -1
				reply.Success = false
				Debug(dAppend, "[%d] R%d REFUSE <- R%d, CONFLICT", currentTerm, me, leaderID)
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

				Debug(dAppend, "[%d] R%d REFUSE <- R%d, CONFLICT", currentTerm, me, leaderID)
				return
			}
			i, j = prevLogIndex+1, 0
		}
	} else { // 含有快照的一致性检查

		// RPC日志的第一个条目就是快照
		lastIncludeIndex := args.LastIncludeIndex - rf.RWLog.SnapshotIndex
		if lastIncludeIndex < 0 { // RPC的快照过时了

			lastIncludeIndex = -lastIncludeIndex   // FOLLOWER日志第一个条目在RPC日志的位置
			if lastIncludeIndex >= len(args.Log) { // 所有的日志条目都是过时的
				Debug(dAppend, "[%d] R%d REFUSE <- R%d, OLD LOG", currentTerm, me, leaderID)
				reply.Success = true
				return
			}
			i, j = 1, lastIncludeIndex+1 // 检查RPC中是否有新的日志可以添加
		} else {

			// FOLLOWER的日志过时啦
			if lastIncludeIndex >= len(rf.Log) {
				rf.Log = args.Log // 删除过时的日志
			} else {
				// 忽略FOLLOWER中过时的日志
				log := make([]Entry, len(rf.Log[lastIncludeIndex:])) // 避免内存泄露采用深拷贝
				copy(log, rf.Log[lastIncludeIndex:])
				rf.Log = log
				rf.Log[0] = args.Log[0] // 直接复制RPC的快照，删除FOLLOWER中过时的日志
			}

			// 接受了RPC快照之后更新snapshotIndex
			rf.RWLog.SnapshotIndex = args.LastIncludeIndex
			i, j = 0, 0
		}
	}

	// 通过了日志的一致性检查，开始追加日志
	reply.Success = true

	// 添加日志后，可能需要更新commitIndex
	// 这里不需要加锁，因为只要不减小commitIndex都是安全的; (减小commitIndex，可能让一条日志条目提交两次)
	defer func() {

		var (
			leaderCommit   = args.LeaderCommit
			newCommitIndex int
		)

		if !args.HasSnapshot {
			newCommitIndex = min(leaderCommit, args.PrevLogIndex+len(args.Log))
		} else {
			newCommitIndex = min(leaderCommit, args.LastIncludeIndex+len(args.Log)-1)
		}

		var commitIndex int
		commitIndex = int(atomic.LoadInt64((*int64)(unsafe.Pointer(&rf.commitIndex))))

		// 因为申请了读锁，server的状态不会发生改变
		// 只有在commitIndex < newCommitIndex时才更新保证了commitIndex的单调性，也保证了正确性
		// 使用CAS是为了保证并发安全，否则可能覆盖别的协程提交的修改
		if commitIndex < newCommitIndex && atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&rf.commitIndex)), int64(commitIndex), int64(newCommitIndex)) {

			// 一边去提交就好了
			go func() {
				// 通知commit协程提交日志
				Debug(dCommit, "[%d] R%d UPDATE  CI:%d <-R%d", currentTerm, me, newCommitIndex, leaderID)
				rf.commitCh <- newCommitIndex
			}()
		}
	}()

	// 响应AE RPC之前要将添加的日志持久化
	defer func() {
		rf.persist()
	}()

	for ; i < len(rf.Log) && j < len(args.Log); i, j = i+1, j+1 {
		// 日志term不匹配，或者term匹配但是一个是快照，一个是日志也不行.
		if rf.Log[i].Term != args.Log[j].Term || (rf.Log[i].CommandValid == args.Log[j].SnapshotValid && rf.Log[i].CommandValid) {
			break
		}
	}

	// 日志添加完成
	if j >= len(args.Log) {
		Debug(dAppend, "[%d] R%d APPEND <- R%d, LEN:%d", currentTerm, me, leaderID, len(args.Log))
		return
	}

	// 出现冲突日志，丢弃掉后续日志
	if i < len(rf.Log) {
		log := make([]Entry, len(rf.Log[:i]))
		copy(log, rf.Log[:i])
		rf.Log = log
		Debug(dAppend, "[%d] R%d DROP <- R%d, CLI:%d-%d, HAS:%v", currentTerm, me, leaderID, i, j, args.HasSnapshot)
	}

	// 追加剩余日志
	rf.Log = append(rf.Log, args.Log[j:]...)

	Debug(dAppend, "[%d] R%d APPEND <- R%d, LEN:%d", currentTerm, me, leaderID, len(args.Log))
}

// Aerpc 向标号为peerIndex的peer发送AE RPC，并处理后续响应
// image		发送RPC时server的Image实例
// nextIndex	发送RPC时peer的nextIndex值
// matchIndex	发送RPC时peer的matchIndex值
// args			RPC参数
func aerpc(image Image, peerIndex int, nextIndex, matchIndex int, args *AppendEntriesArgs) {
	Debug(dAppend, "[%d] R%d AE RPC -> R%d", image.CurrentTerm, image.me, peerIndex)

	reply := new(AppendEntriesReply)
	image.peers[peerIndex].Call("Raft.AppendEntries", args, reply)

	// 无效的响应，或者server的状态已经发生改变，就放弃当前的任务
	if image.Done() || !reply.Valid {
		return
	}
	Debug(dAppend, "[%d] R%d AE <-REPLY R%d, V:%v S:%v CLI:%d, CLT:%d", image.CurrentTerm, image.me, peerIndex, reply.Valid, reply.Success, reply.ConflictIndex, reply.ConflictTerm)

	// server应该转为FOLLOWER，但不用重置计时器
	if reply.Term > image.CurrentTerm {
		image.Update(func(i *Image) {

			// 设置新的Image
			i.State = FOLLOWER
			i.CurrentTerm = reply.Term
			i.VotedFor = -1
			Debug(dTimer, "[%d] R%d CONVERT FOLLOWER <- R%d NEW TERM.", i.CurrentTerm, i.me, peerIndex)

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

	// 避免LEADER的状态发生改变
	image.mu.RLock()
	defer image.mu.RUnlock()
	if image.Done() {
		return
	}

	// 避免LEADER的日志压缩带来的问题
	image.RWLog.mu.RLock()
	defer image.RWLog.mu.RUnlock()

	// 日志匹配不成功
	if !reply.Success {

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

	} else {
		// 日志匹配的情况
		newNextIndex += len(args.Log)
		newMatchIndex = newNextIndex - 1
	}

	// 不需要更新nextIndex，matchIndex
	if nextIndex == newNextIndex && matchIndex == newMatchIndex {
		return
	}

	// 采用CAS的方式，保证并发情况下nextIndex、matchIndex的正确性
	if atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&image.nextIndex[peerIndex])), int64(nextIndex), int64(newNextIndex)) &&
		atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&image.matchIndex[peerIndex])), int64(matchIndex), int64(newMatchIndex)) {
		Debug(dAppend, "[%d] R%d UPDATE M&N -> R%d, MI:%d, NI:%d", image.CurrentTerm, image.me, peerIndex, newMatchIndex, newNextIndex)

		// 通知LEADER提交日志
		if reply.Success {
			image.Raft.updateCommitIndex(image, newMatchIndex)
		}
	}
}

// 当某个FOLLOWER的matchIndex更新之后，计算新的commitIndex；
// 如果commitIndex更新之后，立即提交LEADER的日志
func (rf *Raft) updateCommitIndex(image Image, matchIndex int) {

	// 找到首个能提交的日志条目，更新commitIndex
loop:
	for {
		count := 0
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= matchIndex {
				count++
			}
			// 找到首个大多数server均接受的日志条目时就不用再向前找了
			if count >= len(rf.peers)/2 {
				break loop
			}
		}
		matchIndex--
	}

	commitIndex := rf.commitIndex

	// 一方面：commitIndex 的更新必须保证单调性；另一方面，commitIndex >= snapshoyIndex，可以避免越界问题
	// 不能提交其它任期的entry
	if matchIndex <= commitIndex || rf.Log[matchIndex-rf.RWLog.SnapshotIndex].Term != rf.CurrentTerm {
		return
	}

	// 保证commitIndex的单调性
	if !image.Done() && atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&rf.commitIndex)), int64(commitIndex), int64(matchIndex)) {

		// 通知applier协程提交日志，放在协程内执行可以让calculateCommitIndex尽快返回
		go func() {
			Debug(dCommit, "[%d] R%d UPDATE CI:%d", rf.CurrentTerm, rf.me, matchIndex)
			rf.commitCh <- matchIndex
		}()
	}
}

func (rf *Raft) SendAppendEntries() {
	image := *rf.Image

	Debug(dAppend, "[%d] R%d SEND AE RPC.", rf.CurrentTerm, rf.me)
	rf.RWLog.mu.RLock()
	defer rf.RWLog.mu.RUnlock()
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

		snapshotIndex := rf.RWLog.SnapshotIndex
		// 当nextIndex<=snapshotIndex时发送快照，
		if nextIndex <= snapshotIndex {
			nextIndex = snapshotIndex
			rf.nextIndex[server] = nextIndex // 更新nextIndex

			args.HasSnapshot = true
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

		go aerpc(image, server, nextIndex, matchIndex, args)
	}
}
