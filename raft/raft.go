package raft

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

func init() {
	rand.Seed(time.Now().Unix())
}

const (
	FOLLOWER  int = iota
	CANDIDATE int = iota
	LEADER    int = iota
)

type signal struct{}

type Raft struct {
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	dead chan signal

	// 修改server状态时，要申请写锁
	mu sync.RWMutex

	*Image	// server的状态:CurrentTerm、State、VotedFor
	timer *time.Timer

	*RWLog // 并发安全的日志条目数组

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	commitCh    chan int

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.CurrentTerm, rf.State == LEADER
}

func (rf *Raft) persist() {
	rf.RWLog.mu.RLock()
	defer rf.RWLog.mu.RUnlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log) // 这里包含日志条目与快照
	state := w.Bytes()

	// 单独存储快照
	w = new(bytes.Buffer)
	e = labgob.NewEncoder(w)
	e.Encode(rf.Log[0])
	snapshot := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
	Debug(dPersist, "[%d] S%d SAVE STATE, VF:%d, SI:%d, Log:%v", rf.CurrentTerm, rf.me, rf.VotedFor, rf.RWLog.SnapshotIndex, rf.RWLog.String())
}

func (rf *Raft) readPersist() {

	if rf.persister.RaftStateSize() == 0 {
		rf.VotedFor = -1

		// 占位
		rf.Log = append(rf.Log, Entry{
			ApplyMsg: ApplyMsg{
			},
			Term: -1,
		})
		return
	}

	if rf.persister.RaftStateSize() > 0 {
		state := rf.persister.ReadRaftState()
		r := bytes.NewBuffer(state)
		d := labgob.NewDecoder(r)

		if err := d.Decode(&rf.CurrentTerm); err != nil {
			Debug(dError, "S%d Read CT failed, err:%v", rf.me, err)
		}

		if err := d.Decode(&rf.VotedFor); err != nil {
			Debug(dError, "S%d Read VF failed, err:%v", rf.me, err)
		}

		if err := d.Decode(&rf.Log); err != nil {
			Debug(dError, "S%d Read LOG failed, err:%v", rf.me, err)
		}

		rf.RWLog.SnapshotIndex = rf.Log[0].SnapshotIndex
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// 避免leader发生状态转换
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	if rf.killed() || rf.State != LEADER {
		return -1, -1, false
	}
	term = rf.CurrentTerm
	isLeader = true

	// 并发添加日志时申请logMu，确保index的正确性
	rf.RWLog.mu.Lock()
	defer rf.RWLog.mu.Unlock()

	index = len(rf.Log) + rf.RWLog.SnapshotIndex

	rf.Log = append(rf.Log, Entry{
		ApplyMsg: ApplyMsg{Command: command, CommandIndex: index, CommandValid: true},
		Term:     rf.CurrentTerm,
		Index:    index,
	})

	Debug(dClient, "[%d] S%d APPEND ENTRY. IN:%d, TE:%d， CO:%v", rf.CurrentTerm, rf.me, index, rf.Log[index-rf.RWLog.SnapshotIndex].Term, command)

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	close(rf.dead)
}

func (rf *Raft) killed() bool {

	select {
	case <-rf.dead:
		return true
	default:
		return false
	}
}

const HEARTBEAT = 100 * time.Millisecond

// 选举超时时间至少是心跳时间的3倍
func electionTime() time.Duration {
	d := rand.Intn(300) + 300
	return time.Duration(d) * time.Millisecond
}

// 收到新LEADER的AE PRC、选举计时器到期、投出选票时才重置计时器
func (rf *Raft) resetTimer() {
	// 清空 re.timer.C
	rf.timer.Stop()
	if len(rf.timer.C) > 0 {
		<-rf.timer.C
	}

	switch rf.State {
	case FOLLOWER:
		ELT := electionTime()
		rf.timer.Reset(ELT)
		Debug(dTimer, "[%d] S%d CONVERT FOLLOWER, ELT:%d", rf.CurrentTerm, rf.me, ELT.Milliseconds())
	case CANDIDATE:
		ELT := electionTime()
		rf.timer.Reset(ELT)
		Debug(dTimer, "[%d] S%d CONVERT CANDIDATE, ELT:%d", rf.CurrentTerm, rf.me, ELT.Milliseconds())
	case LEADER:
		rf.timer.Reset(HEARTBEAT)
	}
}

func (rf *Raft) ticker() {

	for {
		select {
		case <-rf.dead:
			Debug(dKill, "[%d] S%d BE KILLED", rf.CurrentTerm, rf.me)
			close(rf.done) // 通知所有的工作协程退出
			rf.timer.Stop()
			rf.commitCh <- -1 // 关闭commit协程，避免内存泄漏
			return
		case f := <-rf.Image.update: // 工作协程通知ticker协程更新server状态
			rf.mu.Lock()             // 更新server状态时申请写锁，避免读写冲突
			f(rf.Image)              // 更新server状态，可能会重置计时器
			rf.Image.did <- signal{} // 通知工作协程，update函数已执行

			// 当server从其他状态变为leader时，初始化matchIndex、nextIndex
			if rf.Image.State == LEADER {
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				rf.RWLog.mu.RLock()
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.Log) + rf.RWLog.SnapshotIndex
				}
				rf.RWLog.mu.RUnlock()
			}
			rf.mu.Unlock()

		case <-rf.timer.C:
			rf.mu.Lock()
			// 选举计时器超时，server的状态转化为CANDIDATE
			if rf.State != LEADER {
				rf.State = CANDIDATE
				rf.CurrentTerm++
				rf.VotedFor = rf.me
				// server的状态发生改变，原来的Image虽之失效
				Debug(dTimer, "[%d] S%d CLOSE image.done", rf.CurrentTerm, rf.me)
				close(rf.Image.done)
				rf.Image.done = make(chan signal)
			}
			// 重置计时器
			rf.resetTimer()
			rf.mu.Unlock()
		}
		// 改变状态之后需要持久化保存
		rf.persist()

		// 执行后续动作
		// 在ticker协程中对状态的读操作不存在读写冲突，没有必要加锁
		switch rf.State {
		case FOLLOWER:
		case CANDIDATE:
			rf.sendRequestVote()
		case LEADER:

			// leader 任期开始时添加一个空的日志条目 no-op 条目
			// if rf.Log[len(rf.Log)-1].Term != rf.CurrentTerm {
			// 	index := len(rf.Log)
			//
			// 	rf.Log = append(rf.Log, Entry{
			// 		ApplyMsg: ApplyMsg{Command: nil, CommandIndex: index},
			// 		Term:     rf.CurrentTerm,
			// 	})
			// 	Debug(dAppend, "[%d] S%d Append Entry. IN:%d, TE:%d", rf.CurrentTerm, rf.me, index, rf.Log[index].Term)
			// }
			// rf.timer.Reset(HEARTBEAT)
			rf.SendAppendEntries()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = make(chan signal)
	// 创建用来提交日志的协程
	rf.commitCh = make(chan int)

	rf.Image = &Image{
		update: make(chan func(*Image)),
		done:   make(chan signal),
		did:    make(chan signal),
		Raft:   rf,
	}
	rf.RWLog = &RWLog{Log: make([]Entry, 0)}
	// initialize from State persisted before a crash
	rf.readPersist()
	rf.timer = time.NewTimer(electionTime())

	go rf.ticker()
	go rf.applier()

	// start ticker goroutine to start elections
	Debug(dTest, "[%d] S%d START.", rf.CurrentTerm, rf.me)

	return rf
}
