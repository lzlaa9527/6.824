package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	ID          int  // 投票人ID
	Valid       bool // 当RPC失败，或者投票者在处理RV PRC过程中状态发生改变，置Invalid为false
}

//
// 在RequestVote执行开始记录某一时刻了server的缓存，我们通过缓存判断那一时刻server是否应该投票
// 在进行选举条件的估值时，如果根据缓存确定应该投反对票，就直接投反对票；但是，如果估值投赞成票，不
// 能就确定应该投赞成票，因为server的状态可能已经发生改变，或者已经向其它的server投过票了，此时选
// 票无效；
//
// 因此为了避免重复投票，在投赞同票时应该使得当前的Image失效
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.RLock()
	// 获取server当前状态的镜像
	image := *rf.Image

	// 这里加锁其他修改rf.log的协程就会被阻塞，因此保证lastLogIndex、lastLogTerm的一致性
	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[lastLogIndex].Term
	rf.mu.RUnlock()

	var (
		currentTerm = image.CurrentTerm
		term        = args.Term
		votedFor    = image.VotedFor
		me          = rf.me
		candidateID = args.CandidateId
	)

	Debug(dVote, "[%d] S%d RV <- S%d", currentTerm, me, candidateID)

	reply.ID = me
	reply.Valid = true
	// reply.term取较大值
	reply.Term = max(term, currentTerm)

	// 参选者的term过时了，投反对票
	if term < currentTerm {
		reply.VoteGranted = false // 通知CANDIATE更新自己的term
		Debug(dVote, "[%d] S%d Refuse -> S%d, Old Term", currentTerm, me, candidateID)
		return
	}

	// CANDIATE.term比较新，任何SERVER都应该更新自己的term；但是不一定会投票还应进行限制选举条件的检查
	if term > currentTerm {

		reply.Valid = image.Update(func(i *Image) {

			// 设置新的Image
			i.State = FOLLOWER
			i.CurrentTerm = term
			i.VotedFor = candidateID

			// 使之前的Image失效
			close(i.done)
			// 使新Image生效
			i.done = make(chan signal)

			// 接下来还需要进行限制选举条件的检查
			// 所以要同步更新当前的image状态
			image = *i
			lastLogIndex = len(rf.Log) - 1
			lastLogTerm = rf.Log[lastLogIndex].Term
			Debug(dVote, "[%d] S%d Convert FOLLOWER <- S%d, New Term", term, rf.me, candidateID)
		})

		// server状态发生了改变
		if !reply.Valid {
			return
		}

	}

	currentTerm = image.CurrentTerm
	votedFor = image.VotedFor

	// 已经没有票了
	if votedFor != -1 && votedFor != candidateID {
		reply.VoteGranted = false
		Debug(dVote, "[%d] S%d Refuse -> S%d, No Vote.", currentTerm, me, candidateID)
		return
	}

	// 满足限制选举条件
	// 1. 如果CANDIDATE的最后的日志条目Term更大，投票
	// 2. 如果Term一样大，但是CANDIDATE的日志更长，投票
	// 3. 如果CANDIDATE与FOLLOWER的日志一样新，也投票
	if args.LastLogTerm > lastLogTerm || (args.LastLogIndex >= lastLogIndex && args.LastLogTerm == lastLogTerm) {
		reply.VoteGranted = true

		Debug(dVote, "[%d] S%d SAT -> S%d", currentTerm, me, candidateID)

		// 注意赞成票只有在server状态没有发生改变时才有效
		reply.Valid = image.Update(func(i *Image) {

			// 设置新的Image
			i.State = FOLLOWER
			i.VotedFor = candidateID

			// 先使得之前的Image失效
			close(i.done)
			// 使新Image生效
			i.done = make(chan signal)
			Debug(dVote, "[%d] S%d Vote -> S%d, New Entries.", currentTerm, me, candidateID)
		})
		return
	}
}

// 用来计票的工作协程
func votesCounter(image Image, repliesCh <-chan *RequestVoteReply) <-chan signal {
	servers := len(image.peers)
	done := make(chan signal)
	go func() {
		done <- signal{} // 计票协程已建立

		n := 0            // 一共获得的票数
		votesCounter := 0 // 统计获得的赞成票
		for reply := range repliesCh {

			n++ // 获得一张选票

			// 如果server的状态已经发生改变，也不用统计票数了。
			// 但是不能直接退出协程，不然向repliesCh发送选票的协程会被阻塞
			if image.Done() {
				goto check
			}
			// 处理有效票
			if reply.Valid {

				// 获得一张反对票
				if !reply.VoteGranted {

					// server应该转为follower
					if reply.Term > image.CurrentTerm {
						image.Update(func(i *Image) {

							// 设置新的Image
							i.State = FOLLOWER
							i.VotedFor = -1
							i.CurrentTerm = reply.Term

							// 使旧的Image失效
							close(i.done)
							// 使新Image生效
							i.done = make(chan signal)
							Debug(dTimer, "[%d] S%d Convert FOLLOWER <- S%d New Term.", i.CurrentTerm, i.me, reply.ID)

						})
						return
					}
					goto check
				}

				// 获得一张赞成票
				votesCounter++

				if votesCounter+1 > servers/2 {
					// 通知ticker协程将servre的状态更新为leader
					image.Update(func(i *Image) {

						// 设置新的Image
						i.State = LEADER

						// 令旧的Image失效
						close(i.done)
						// 使新Image生效
						i.done = make(chan signal)
						Debug(dVote, "[%d] S%d Convert LEADER.", i.CurrentTerm, i.me)

					})
					return
				}
			}
		check:
			if n == servers-1 {
				// server落选
				break
			}
		}
	}()
	return done
}

func (rf *Raft) sendRequestVote(image Image) {

	replysCh := make(chan *RequestVoteReply)

	n := len(rf.Log)
	args := &RequestVoteArgs{
		Term:         image.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: n - 1,
		LastLogTerm:  rf.Log[n-1].Term,
	}

	// 先开启计票协程再开始选举
	<-votesCounter(image, replysCh)

	// 开始选举
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := new(RequestVoteReply)

			if image.Done() {
				// 使得votesCounter协程正常关闭，避免内存泄漏
				// votesCounter 必须接受到足够的选票（无论是否有效的）才会关闭
				replysCh <- reply
				return
			}

			Debug(dVote, "[%d] S%d RV -> S%d", image.CurrentTerm, rf.me, server)

			// 将选票结果发送给计票协程
			rf.peers[server].Call("Raft.RequestVote", args, reply)

			replysCh <- reply // 无论选票是否有效都要交给计票协程，目的是让计票协程能够正确返回
		}(server)

	}

}
