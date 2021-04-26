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

//
// A Go object implementing a single Raft peer.
//

const (
	FOLLOWER  int = iota
	CANDIDATE int = iota
	LEADER    int = iota
)

type signal struct{}

type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted State
	me        int                 // this peer's index into peers[]

	dead chan signal

	// 修改server状态时，要申请写锁
	mu sync.RWMutex

	// Image 存储server某个状态下的一个镜像，Image具有时效性，当server的状态发生更改,Image也可能会失效了。
	//
	// 计时器超时以及某些事件可能改变server的状态，此时sever需要更新自己的状态以及执行一些列动作：
	// 		如重置计时器、发起选举、发出心跳等；
	//
	// timer.C 	 		 用来监听计时器超时时间
	// Image.Done() 	 用来监听其它的能改变server状态的事件
	//
	// 除了计时器超时外，改变server状态的事件有：
	// 		收到leader的AP RPC，投出自己的选票，获得足够的选票，发现自己的term落后了；
	// 其中需要注意的是除了收到新leader的AP RPC之外，上述事件发生时会使当前的Image失效，因此那些监听该
	// Image的工作协程需要放弃手头的工作。
	//
	// 当server发生了上述事件时工作协程就通过Image的update管道通知ticker协程；工作协程会传送一个用来更新server
	// 状态的update函数，如上所述部分update函数可能会使得当前的Image失效；ticker协程会执行该update函数，并执
	// 行后续动作。
	//
	// 因此，每个工作协程在访问server的状态要先判断Image是否还有效，如果Image已失效就表示server的状态发生
	// 改变应该放弃当前的工作；这样可以避免破坏一致性的修改；以及通过监听Image来判断server状态是否改变，也可
	// 以减少申请rf.mu的争用。
	//
	*Image
	timer *time.Timer

	*RWLog // 并发安全的日志条目数组

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	commitCh    chan int

	nextIndex  []int
	matchIndex []int
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

//
// restore previously persisted State.
//
func (rf *Raft) readPersist() {

	// Your code here (2C).
	// bootstrap without any State?
	if rf.persister.RaftStateSize() == 0 {
		rf.VotedFor = -1

		// 占位符
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
	case <-rf.done:
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
	// 重置 re.timer.C
	rf.timer.Stop()
	if len(rf.timer.C) > 0 {
		select {
		case <-rf.timer.C:
		default:
		}
	}

	switch rf.State {
	case FOLLOWER:
		ELT := electionTime()
		// 选举时间[200,450]ms
		rf.timer.Reset(ELT)
		Debug(dTimer, "[%d] S%d CONVERT FOLLOWER, ELT:%d", rf.CurrentTerm, rf.me, ELT.Milliseconds())
	case CANDIDATE:
		ELT := electionTime()
		rf.timer.Reset(ELT)
		Debug(dTimer, "[%d] S%d CONVERT CANDIDATE, ELT:%d", rf.CurrentTerm, rf.me, ELT.Milliseconds())
	case LEADER:
		rf.timer.Reset(HEARTBEAT)
		// rf.SendAppendEntries(*rf.Image)
	}
}

// 当计时器超时或者收到工作协程通知需要改变server状态时，更新server的image以及执行后续动作
func (rf *Raft) ticker() {

	for {
		select {
		case <-rf.dead:
			Debug(dKill, "[%d] S%d BE KILLED", rf.CurrentTerm, rf.me)
			close(rf.done)
			rf.timer.Stop()
			rf.commitCh <- -1 // 关闭commit协程，避免内存泄漏
			return
		case f := <-rf.Image.update:
			rf.mu.Lock()

			f(rf.Image)
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

			if rf.State != LEADER {
				// server是follower、candidate，定时器超时转为candidate
				rf.State = CANDIDATE
				rf.CurrentTerm++
				rf.VotedFor = rf.me

				// 只要计时器超时时server不是leader那就意味着需要更新server的Image
				//
				// 先关闭当前的Image再更新状态
				// 其它工作协程监听到Image失效之后，应该放弃手头的工作
				Debug(dTimer, "[%d] S%d CLOSE image.done", rf.CurrentTerm, rf.me)
				close(rf.Image.done)
				rf.Image.done = make(chan signal)
			}

			// 重置计时器
			rf.resetTimer()
			rf.mu.Unlock()
		}

		rf.persist()

		// 执行后续动作

		// 下列操作中对server状态的读操作，都不会产生data-race
		// 因为对server状态的写操作在当前协程内
		switch rf.State {
		case FOLLOWER:
		case CANDIDATE:
			rf.sendRequestVote(*rf.Image)
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
			rf.SendAppendEntries(*rf.Image)
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

	// 创建用来提交日志的协程
	rf.commitCh = make(chan int)
	go rf.applier()

	// start ticker goroutine to start elections
	Debug(dTest, "[%d] S%d START.", rf.CurrentTerm, rf.me)

	return rf
}
