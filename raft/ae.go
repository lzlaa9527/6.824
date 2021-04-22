package raft

import (
	"sync/atomic"
	"unsafe"
)

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).

	Term         int // leader's CurrentTerm
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of Log entry immediately preceding new ones
	PrevLogTerm  int // CurrentTerm of prevLogIndex entry
	Log          []Entry
	LeaderCommit int // leader's commitIndex
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
	// 获取server当前状态的镜像，加锁是为了避免在复制过程中状态发生改变，产生不一致；（理论上，这是可能发生的）

	image := rf.GetImage()

	var (
		me          = rf.me
		term        = args.Term
		currentTerm = image.CurrentTerm
		leaderID    = args.LeaderId
	)

	reply.Valid = image.Done()
	reply.Term = max(term, currentTerm) // 响应的term一定是较大值

	Debug(dAppend, "[%d] S%d RECEIVE<- S%d AE,T:%d PLI:%d PLT:%d LEN:%d", currentTerm, me, leaderID, term, args.PrevLogIndex, args.PrevLogTerm, len(args.Log))

	if term < currentTerm {
		reply.Success = false
		Debug(dAppend, "[%d] S%d Refuse <- S%d, Lower Term.", currentTerm, me, leaderID)
		return
	}

	// 如果不缓存log而是直接用rf.Log做检查，可能发生越界异常；
	// 因为该处理协程可能会被延迟，而别的处理协程可能已经删除了部分log
	// 记录server当前的日志，用于日志匹配条件的检查；
	var log []Entry

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
		log = i.RWLog.Log

		Debug(dAppend, "[%d] S%d Convert FOLLOWER <- S%d, RESET TIME.", term, me, leaderID)

		i.resetTimer() // 重置计时器
	})

	// server的状态已经发生了改变
	if !reply.Valid {
		return
	}

	// server的状态已经改变
	currentTerm = image.CurrentTerm

	var (
		prevLogIndex = args.PrevLogIndex
		prevLogTerm  = args.PrevLogTerm
	)

	if prevLogIndex >= len(log) {
		reply.ConflictIndex = len(log)
		reply.ConflictTerm = -1
		reply.Success = false
		Debug(dAppend, "[%d] S%d Refuse <- S%d", currentTerm, me, leaderID)

		return
	}

	// RWLog[prevLogIndex].Term != prevLogTerm 找到ConflictIndex然后返回false
	if prevLogTerm != log[prevLogIndex].Term {
		reply.Success = false
		reply.ConflictTerm = log[prevLogIndex].Term
		// 找到首个term为ConflictTerm的ConflictIndex
		for reply.ConflictIndex = prevLogIndex; log[reply.ConflictIndex].Term == reply.ConflictTerm; reply.ConflictIndex-- {
		}
		reply.ConflictIndex++

		// 如果server状态已经改变，上述数据是无效的
		reply.Valid = !image.Done()
		Debug(dAppend, "[%d] S%d Refuse <- S%d", currentTerm, me, leaderID)
		return
	}

	reply.Success = true

	// append日志

	// 要保证追加日志时server的状态没有发生改变，所以这里要加读锁
	// 如果不加锁server状态可能发生改变，有可能删除其它leader添加的已提交的日志
	rf.mu.RLock()
	if image.Done() {
		reply.Valid = false
		rf.mu.RUnlock()
		return
	}

	// 下面开始添加日志，可能需要更新commitIndex
	// 这里不需要加锁，因为只要不减小commitIndex都是安全的; 减小commitIndex，可能让一条日志条目提交两次
	defer func() {
		leaderCommit := args.LeaderCommit
		newCommitIndex := min(leaderCommit, prevLogIndex+len(args.Log))

		// 判断commitIndex是否需要更新
		var commitIndex int
		commitIndex = int(atomic.LoadInt64((*int64)(unsafe.Pointer(&rf.commitIndex))))

		// 只有在commitIndex < newCommitIndex时才更新；使用CAS是为了保证并发安全
		if commitIndex < newCommitIndex && atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&rf.commitIndex)), int64(commitIndex), int64(newCommitIndex)) {

			// 一边去提交就好了
			go func() {
				// 通知commit协程提交日志
				Debug(dCommit, "[%d] S%d Update  CI:%d <-S%d", currentTerm, me, newCommitIndex, leaderID)
				rf.commitCh <- newCommitIndex
			}()
		}
	}()

	// 响应AE RPC之前要将添加的日志先持久化
	defer func() {
		rf.mu.RLock()
		rf.persist()
		rf.mu.RUnlock()
	}()

	// 为了保证并发修改日志的正确性，这里申请日志写锁
	rf.RWLog.mu.Lock()
	// defer rf.persist()
	i, j := prevLogIndex+1, 0
	for ; i < len(rf.Log) && j < len(args.Log); i, j = i+1, j+1 {
		if rf.Log[i].Term != args.Log[j].Term {
			break
		}
	}

	// 日志添加完成
	if j == len(args.Log) {
		rf.RWLog.mu.Unlock()
		rf.mu.RUnlock()
		Debug(dAppend, "[%d] S%d Append1 <- S%d, LEN:%d", currentTerm, me, leaderID, len(args.Log))
		return
	}

	// 出现冲突日志
	if i < len(rf.Log) {
		rf.Log = rf.Log[:i]
		Debug(dAppend, "[%d] S%d Drop <- S%d, CLI:%d", currentTerm, me, leaderID, i)
	}

	// 追加剩余日志
	rf.Log = append(rf.Log, args.Log[j:]...)

	rf.RWLog.mu.Unlock()
	rf.mu.RUnlock()
	Debug(dAppend, "[%d] S%d Append2 <- S%d, LEN:%d", currentTerm, me, leaderID, len(args.Log))

}

func aerpc(image Image, server int, args *AppendEntriesArgs, nextIndex, matchIndex int, entries []Entry) {

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
			Debug(dTimer, "[%d] S%d Convert FOLLOWER <- S%d New Term.", i.CurrentTerm, i.me, server)

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
		// 1. 在数据提交前会验证server状态是否改变，只有未改变时才能提交
		// 2. leader只会添加日志，不会删除和修改自己的日志条目
		// 因此这里不用申请锁
		log := image.RWLog.Log

		// leader日志中不存在Term与reply.ConflictTerm相同的日志条目，将nextIndex设为conflictTerm
		if reply.ConflictTerm == -1 {
			newNextIndex = reply.ConflictIndex
		} else {
			newNextIndex = reply.ConflictIndex

			// 如果log[newNextIndex].Term != reply.ConflictTerm，
			// 那么newNextIndex左边的日志也不可能与server的日志匹配
			if log[newNextIndex].Term == reply.ConflictTerm {
				for ; newNextIndex < args.PrevLogIndex && log[newNextIndex].Term == reply.ConflictTerm; newNextIndex++ {
				}
			}
		}
	} else {
		// 日志匹配的情况
		newNextIndex += len(entries)
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
		Debug(dAppend, "[%d] S%d Update M&N -> S%d, MI:%d, NI:%d", image.CurrentTerm, image.me, server, newMatchIndex, newNextIndex)
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
	// Log 可能会发生数据冲突，但是不影响正确性；因为此时server一定是LEADER，他不会删除、修改自己的log
	if rf.Log[newCommitIndex].Term != rf.CurrentTerm {
		return
	}

	// 更新commitIndex
	if atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&rf.commitIndex)), int64(oldCommitIndex), int64(newCommitIndex)) {
		// 一边慢慢去提交就好了
		go func() {
			Debug(dCommit, "[%d] S%d Update CI:%d", rf.CurrentTerm, rf.me, newCommitIndex)
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

		// 只要server的状态不发生改变，这些数据就是有效的
		var (
			nextIndex    = rf.nextIndex[server]
			matchIndex   = rf.matchIndex[server]
			prevLogIndex = nextIndex - 1
			prevLogTerm  = rf.Log[prevLogIndex].Term
			entries      = append(rf.Log[:0:0], rf.Log[nextIndex:]...)
		)

		args := &AppendEntriesArgs{
			Term:         image.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
			Log:          entries,
		}

		go aerpc(image, server, args, nextIndex, matchIndex, entries)

	}

}
