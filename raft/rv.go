package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	ID          int  // 投票人ID
	Valid       bool // 当RPC失败，或者投票者在处理RV PRC过程中状态发生改变，置Invalid为false
}

//
// 每当server收到RV RPC请求后都会创建RequestVote协程处理RPC请求，检查选举条件。
// 在处理过程中server的状态可能会发生改变，处理这种情况的一个基本原则：如果创建协程时
// 刻RPC参数不满足选举条件应直接返回投反对票，但是创建协程时RPC请求参数满足选举条件并
// 不能直接投赞成票，因为在投票之前server的状态可能已经转变了，此时的选票应该是无效的。
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// 这里加锁可以保证Image实例和lastEntry的一致性
	rf.mu.RLock()
	// 获取server当前状态的镜像
	image := *rf.Image
	rf.RWLog.mu.RLock()
	lastEntry := rf.RWLog.Log[len(rf.Log)-1]
	rf.RWLog.mu.RUnlock()
	rf.mu.RUnlock()

	var (
		currentTerm  = image.CurrentTerm
		term         = args.Term
		votedFor     = image.VotedFor
		me           = rf.me
		candidateID  = args.CandidateId
		lastLogIndex = lastEntry.Index
		lastLogTerm  = lastEntry.Term
	)

	// reply.term取较大值
	reply.Term = Max(term, currentTerm)
	reply.Valid = true
	Debug(DVote, "[%d] R%d#%d RECEIVE<- R%d,T:%d LLI:%d LLT:%d", currentTerm, me, rf.gid, candidateID, term, args.LastLogIndex, args.LastLogTerm)

	// CANDIDATE的term过时了，投反对票
	if term < currentTerm {
		reply.VoteGranted = false // 通知CANDIATE更新自己的term
		Debug(DVote, "[%d] R%d#%d REFUSE -> R%d, OLD TERM", currentTerm, me, rf.gid, candidateID)
		return
	}

	// CANDIDATE.term比较新，任何SERVER都应该更新自己的term；
	// 但是不一定会投票还应进行限制选举条件的检查
	if term > currentTerm {

		reply.Valid = image.Update(func(i *Image) {
			// 设置新的Image
			i.State = FOLLOWER
			i.CurrentTerm = term
			i.VotedFor = -1

			// 使之前的Image实例失效
			close(i.done)
			// 使新Image实例生效
			i.done = make(chan signal)

			// 接下来还需要进行限制选举条件的检查
			// 所以要同步更新当前的image状态
			image = *i
			currentTerm = image.CurrentTerm
			votedFor = image.VotedFor
			Debug(DVote, "[%d] R%d#%d CONVERT FOLLOWER <- R%d, NEW TERM", term, rf.me, rf.gid, candidateID)
		})
		// server状态已经发生了改变
		if !reply.Valid {
			return
		}
	}

	// 已经没有票了
	if votedFor != -1 && votedFor != candidateID {
		reply.VoteGranted = false
		Debug(DVote, "[%d] R%d#%d REFUSE -> R%d, NO VOTE.", currentTerm, me, rf.gid, candidateID)
		return
	}

	// 满足限制选举条件，投出赞成票并重置计时器
	// 如果Candidate的最后的日志条目Term更大，投赞成票
	// 如果Term一样大，Candidate的日志更长或者和server的日志一样长，投赞成票

	if args.LastLogTerm > lastLogTerm || (args.LastLogIndex >= lastLogIndex && args.LastLogTerm == lastLogTerm) {
		reply.VoteGranted = true
		// 注意赞成票只有在server状态没有发生改变时才有效
		reply.Valid = image.Update(func(i *Image) {

			// 设置新的Image实例
			i.State = FOLLOWER
			i.VotedFor = candidateID

			// 先使得之前的Image实例失效
			close(i.done)
			// 使新Image实例生效
			i.done = make(chan signal)
			Debug(DVote, "[%d] R%d#%d VOTE -> R%d", currentTerm, me, rf.gid, candidateID)

			i.resetTimer() // 确定投赞成票后要重置计时器
		})
		return
	}
	// 拒绝投票
	reply.VoteGranted = false
	Debug(DVote, "[%d] R%d#%d REFUSE VOTE -> R%d, Old LOG", currentTerm, me, rf.gid, candidateID)
}

// 用来计票的工作协程，为了保证计票结果得正确性，在获得半数赞成票之前votesCounter
// 协程需要统计所有的票才能退出（停止检票）；同时为了避免发送reply的协程被阻塞，停止
// 检票之后不应该关闭replyCh。
//
// image	创建工作协程时server的状态
// replyCh 	接受其它peers的投票结果
func votesCounter(image Image, replyCh <-chan *RequestVoteReply) <-chan signal {
	servers := len(image.peers)
	done := make(chan signal)
	go func() {
		done <- signal{} // 计票协程已经开始执行了，告知raft发送RV RPC

		// 已经处理的票数，为了保证结果的正确性在获得半数以上赞成票之前必须处理所有的投票结果
		n := 0
		agree := 0 // 统计获得的赞成票数
		for reply := range replyCh {
			n++ // 获得一张选票

			// 如果server的状态已经发生改变，就不用继续处理了
			// 但是不能直接退出协程，不然向replyCh发送选票的协程会被阻塞
			if image.Done() {
				goto check // 判断是否已经收到所有的选票
			}

			// 处理有效票
			if reply.Valid {
				Debug(DVote, "[%d] R%d#%d <-REPLY R%d, V:%v GV:%v T:%d", image.CurrentTerm, image.me, image.gid, reply.ID, reply.Valid, reply.VoteGranted, reply.Term)
				// 获得一张反对票
				if !reply.VoteGranted {
					if reply.Term > image.CurrentTerm {
						// server应该转为follower
						image.Update(func(i *Image) {

							// 设置新的Image
							i.State = FOLLOWER
							i.VotedFor = -1
							i.CurrentTerm = reply.Term

							// 使旧的Image失效
							close(i.done)
							// 使新Image生效
							i.done = make(chan signal)
							Debug(DTimer, "[%d] R%d#%d CONVERT FOLLOWER <- R%d NEW TERM.", i.CurrentTerm, i.me, i.gid, reply.ID)
						})
					}
					goto check
				}

				// 获得一张赞成票
				agree++
				if agree+1 > servers/2 {
					// 通知ticker协程将serve的状态更新为leader
					// 只有当前的Image实例有效时，更新函数才会执行
					image.Update(func(i *Image) {
						rf := i.Raft
						rf.State = LEADER

						// 初始化server的状态
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))

						rf.RWLog.mu.Lock()
						for j := range rf.peers {
							rf.nextIndex[j] = len(rf.Log) + rf.RWLog.SnapshotIndex
							rf.matchIndex[j] = rf.RWLog.SnapshotIndex
						}

						index := len(rf.Log) + rf.RWLog.SnapshotIndex
						rf.Log = append(rf.Log, Entry{
							ApplyMsg: ApplyMsg{Command: nil, CommandIndex: index},
							Term:     rf.CurrentTerm,
							Index:    index,
						})

						rf.RWLog.mu.Unlock()

						// 因为需要改变server的状态，所以应该使之前的Image实例失效
						close(rf.done)
						// 重新绑定一个image.don就能使新Image实例生效
						rf.done = make(chan signal)
						Debug(DVote, "[%d] R%d#%d CONVERT LEADER.", rf.CurrentTerm, rf.me, rf.gid)

						// 重置计时器，设置心跳时间
						rf.resetTimer()
					})
				}
			}
		check:
			if n == servers-1 { // 落选
				break
			}
		}
	}()
	return done
}

//
// 当选举计时器超时之后，Candidate会调用sendRequestVote向其它的peers发送
// RV(Request Vote) RPC请求，当收到RV RPC的响应之后将RequestVoteReply
// 通知votesCounter协程开始计票；
//
func (rf *Raft) sendRequestVote() {
	// 因为sendRequestVote在ticker协程中执行，所以在获取rf的Image实例不会发生读写冲突
	image := *rf.Image

	// 避免对日志读写的并发冲突
	rf.RWLog.mu.RLock()
	lastEntry := rf.RWLog.Log[len(rf.Log)-1]
	rf.RWLog.mu.RUnlock()

	// 对所有peers的参数都是一样的
	args := &RequestVoteArgs{
		Term:         image.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastEntry.Index,
		LastLogTerm:  lastEntry.Term,
	}

	// 先开启计票协程再开始选举
	replysCh := make(chan *RequestVoteReply)
	<-votesCounter(image, replysCh)

	// 开始选举
	Debug(DVote, "[%d] R%d#%d SEND RV RPC", image.CurrentTerm, rf.me, rf.gid)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := new(RequestVoteReply)

			// 选举提前结束
			if image.Done() {
				// 使得votesCounter协程正常关闭，避免内存泄漏
				// votesCounter 必须接受到足够的选票（无论是否有效的）才会关闭
				reply.ID = server
				replysCh <- reply
				return
			}

			rf.peers[server].Call("Raft.RequestVote", args, reply)
			reply.ID = server

			// 无论选票是否有效都要交给计票协程，目的是让计票协程能够正确返回
			replysCh <- reply
		}(server)
	}
}
