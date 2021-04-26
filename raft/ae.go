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

	LastIncludeTerm  int
	LastIncludeIndex int
	Log              []Entry
	Snapshot         bool // 是否包含Snapshot
}

type AppendEntriesReply struct {
	// Your data here (2A).

	Valid bool // 当RPC失败，或者follower在处理AE PRC过程中状态发生改变，置Invalid为false

	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

//
// 在AppendEntries执行开始记录某一时刻了server的缓存，我们通过缓存判断那一时刻server是否应该接受日志。
//
// 在日志匹配条件的估值时，如果日志不匹配直接令reply.Success=false；但是日志匹配不能直接令
// reply.Success=true，还需要判断server的状态是否发生改变，如果发生改变直接令reply.Valid=false并返回;
//
// 因此为了避免重复投票，在投赞同票时应该使得当前的Image失效
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	image := rf.GetImage()

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

	// 收到leader AE RPC，需要成为FOLLOWER并重置自己的计时器
	reply.Valid = image.Update(func(i *Image) {
		i.State = FOLLOWER
		i.CurrentTerm = term

		i.VotedFor = leaderID

		// 在收到AE RPC时只要不是该LEADER的追随者，都应该转为FOLLOWER并放弃工作协程
		// 该LEADER的追随者只用重置选举计时器即可
		if !(term == currentTerm && image.State == FOLLOWER) {

			// 为了以防万一先重置新的管道，在关闭旧的image.done管道
			close(i.done)
			i.done = make(chan signal)
		}
		// 这里需要同时更新image
		image = *i

		Debug(dAppend, "[%d] S%d CONVERT FOLLOWER <- S%d.", term, me, leaderID)

		i.resetTimer() // 重置计时器
	})

	// server的状态已经发生了改变
	if !reply.Valid {
		return
	}

	// server的状态已经改变
	currentTerm = image.CurrentTerm

	// LEADER发来不含快照的日志
	if !args.Snapshot {
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

		if prevLogIndex >= len(rf.Log) {
			reply.ConflictIndex = len(rf.Log)
			reply.ConflictTerm = -1
			reply.Success = false

			Debug(dAppend, "[%d] S%d REFUSE <- S%d, CONFLICT", currentTerm, me, leaderID)
			rf.RWLog.mu.RUnlock()

			reply.Valid = !image.Done()
			return
		}

		// RWLog[prevLogIndex].Term != prevLogTerm 找到ConflictIndex然后返回false
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

	reply.Success = true

	// append日志

	// 要保证追加日志时server的状态没有发生改变，所以这里要加读锁
	// 如果不加锁server状态可能发生改变，有可能删除其它leader添加的已提交的日志
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	reply.Valid = !image.Done()
	if image.Done() {
		return
	}

	// 下面开始添加日志，可能需要更新commitIndex
	// 这里不需要加锁，因为只要不减小commitIndex都是安全的; 减小commitIndex，可能让一条日志条目提交两次
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

		// 只有在commitIndex < newCommitIndex时才更新；使用CAS是为了保证并发安全
		if commitIndex < newCommitIndex && atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&rf.commitIndex)), int64(commitIndex), int64(newCommitIndex)) {

			// 一边去提交就好了
			go func() {
				// 通知commit协程提交日志
				Debug(dCommit, "[%d] S%d UPDATE  CI:%d <-S%d", currentTerm, me, newCommitIndex, leaderID)
				rf.commitCh <- newCommitIndex
			}()
		}
	}()

	// 响应AE RPC之前要将添加的日志先持久化
	defer func() {
		rf.persist()
	}()

	// 为了保证并发修改日志的正确性，这里申请日志写锁
	rf.RWLog.mu.Lock()
	defer rf.RWLog.mu.Unlock()

	// i，j作为指针扫描FOLLOWER与LEADER的日志，进行日志匹配条件的检查
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

func aerpc(image Image, server int, args *AppendEntriesArgs, nextIndex, matchIndex int, n int) {

	reply := new(AppendEntriesReply)
	ok := image.peers[server].Call("Raft.AppendEntries", args, reply)

	if image.Done() {
		return
	}

	//  无效的响应，或者server的状态已经发生改变，就放弃当前的任务
	if !ok || !reply.Valid {
		return
	}

	Debug(dAppend, "[%d] S%d AE <-REPLY S%d, V:%v S:%v CLI:%d, CLT:%d", image.CurrentTerm, image.me, server, reply.Valid, reply.Success, reply.ConflictIndex, reply.ConflictTerm)

	// server应该转为follower，但不用重置计时器
	if reply.Term > image.CurrentTerm {
		image.Update(func(i *Image) {

			// 设置新的Image
			i.State = FOLLOWER
			i.CurrentTerm = reply.Term
			i.VotedFor = -1
			Debug(dTimer, "[%d] S%d CONVERT FOLLOWER <- S%d NEW TERM.", i.CurrentTerm, i.me, server)

			// 使就的Image失效
			close(i.done)
			// 使新Image生效
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
		newNextIndex += n
		newMatchIndex = newNextIndex - 1
	}

	// 要保证上面的数据是有效的，才会更新nextIndex、newMatchIndex
	if image.Done() {
		return
	}

	// 不需要更新nextIndex，matchIndex
	if nextIndex == newNextIndex && matchIndex == newMatchIndex {
		return
	}

	// 更新peers的nextIndex、newMatchIndex
	// 采用CAS的方式，保证并发情况下nextIndex、matchIndex的正确性
	// 即使server的状态已经发生改变不再是leader，修改nextIndex、matchIndex也是无害的

	if atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&image.nextIndex[server])), int64(nextIndex), int64(newNextIndex)) &&
		atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&image.matchIndex[server])), int64(matchIndex), int64(newMatchIndex)) {
		Debug(dAppend, "[%d] S%d UPDATE M&N -> S%d, MI:%d, NI:%d", image.CurrentTerm, image.me, server, newMatchIndex, newNextIndex)
	}

}

// caculateCommitIndex 在leader发送心跳之前计算commitIndex
func (rf *Raft) caculateCommitIndex() {

	// 采用CAS的思想，更新commitIndex，如果在提交之前commitIndex发生改变，就放弃提交；
	// 因此整个过程就没必要加锁了。
	oldCommitIndex := int(atomic.LoadInt64((*int64)(unsafe.Pointer(&rf.commitIndex))))
	newCommitIndex := -1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 计算matchIndex的上限，加速计算出正确的commitIndex
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

	if newCommitIndex < rf.lastApplied {
		return
	}

	// 不能提交其它任期的entry
	rf.RWLog.mu.RLock()
	if rf.Log[newCommitIndex-rf.RWLog.SnapshotIndex].Term != rf.CurrentTerm {
		rf.RWLog.mu.RUnlock()
		return
	}
	rf.RWLog.mu.RUnlock()

	// 更新commitIndex
	if atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&rf.commitIndex)), int64(oldCommitIndex), int64(newCommitIndex)) {
		// 一边慢慢去提交就好了
		go func() {
			Debug(dCommit, "[%d] S%d UPDATE CI:%d", rf.CurrentTerm, rf.me, newCommitIndex)
			rf.commitCh <- newCommitIndex
		}()
	}

}

func (rf *Raft) SendAppendEntries(image Image) {

	rf.caculateCommitIndex()
	Debug(dAppend, "[%d] S%d SEND AE RPC.", rf.CurrentTerm, rf.me)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		// 记录下此时的nextIndex,matchIndex；
		// 在收到RPC响应之后如果nextIndex,matchIndex发生改变，无需更新nextIndex,matchIndex
		var (
			nextIndex  = rf.nextIndex[server]
			matchIndex = rf.matchIndex[server]
		)

		args := &AppendEntriesArgs{
			Term:         image.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

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
		go aerpc(image, server, args, nextIndex, matchIndex, len(args.Log))
	}
}
