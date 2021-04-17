package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, CurrentTerm, isleader)
//   start agreement on a new RWLog entry
// rf.GetState() (CurrentTerm, isLeader)
//   ask a Raft for its current CurrentTerm, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the RWLog, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
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

type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted State
	me        int                 // this peer's index into peers[]
	dead      int32

	mu sync.RWMutex // 计时器超时，server状态转变时，要申请写锁

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

type signal struct{}

//
// save Raft's persistent State to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	rf.mu.RLock()
	defer rf.mu.RUnlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	Debug(dPersist, "[%d] S%d Save State, VF:%d", rf.State, rf.me, rf.VotedFor)
}

//
// restore previously persisted State.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any State?
		rf.VotedFor = -1

		rf.Log = append(rf.Log, Entry{
			Term: -1,
		})
		return
	}

	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.Log); err != nil {
		Debug(dError, "S%d ReadPersist RWLog failed, err: %v", rf.me, err)
	}

	if err := d.Decode(&rf.CurrentTerm); err != nil {
		Debug(dError, "S%d ReadPersist CurrentTerm failed, err: %v", rf.me, err)
	}

	if err := d.Decode(&rf.VotedFor); err != nil {
		Debug(dError, "S%d ReadPersist VoteFor failed, err: %v", rf.me, err)
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
	index = len(rf.Log)

	rf.Log = append(rf.Log, Entry{
		ApplyMsg: ApplyMsg{Command: command, CommandIndex: index},
		Term:     rf.CurrentTerm,
	})

	Debug(dClient, "[%d] S%d Append Entry. IN:%d, TE:%d， CO:%v", rf.CurrentTerm, rf.me, index, rf.Log[index].Term, command)

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// for循环确保正常关闭server
	for {
		done := rf.Image.Update(func(i *Image) {
			close(i.done)
		})
		if done {
			return
		}

	}

}

func (rf *Raft) killed() bool {

	return atomic.LoadInt32(&rf.dead) == 1
}

const HEARTBEAT = 150 * time.Millisecond

func electionTime() time.Duration {
	return time.Duration((rand.Intn(200))+250) * time.Millisecond
}

// 当计时器超时或者收到工作协程通知需要改变server状态时，更新server的image以及执行后续动作
func (rf *Raft) ticker() {

	for {

		select {
		case f := <-rf.Image.update:
			rf.mu.Lock()

			f(rf.Image)
			rf.Image.did <- signal{} // 通知工作协程，update函数已执行

			if rf.killed() {
				Debug(dKill, "[%d] S%d Be killed.", rf.CurrentTerm, rf.me)
				rf.mu.Unlock()
				return
			}
			// 当server从其他状态变为leader时，初始化matchIndex、nextIndex
			if rf.Image.State == LEADER {
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))

				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.Log)
				}
			}
			rf.mu.Unlock()

		case <-rf.timer.C:
			if rf.killed() {
				return
			}

			rf.mu.Lock()

			// 若server是leader, 当定时器超时时，不改变状态
			if rf.State == LEADER {
				rf.mu.Unlock()
				break
			} else {
				// 只要计时器超时时server不是leader那就意味着需要更新server的Image
				//
				// 先关闭当前的Image再更新状态
				// 其它工作协程监听到server状态更新之后，应该放弃手头的工作
				Debug(dTimer, "[%d] S%d Close image.done", rf.CurrentTerm, rf.me)
				close(rf.Image.done)
				rf.Image.done = make(chan signal)
			}

			// server是follower、candidate，定时器超时转为candidate
			rf.State = CANDIDATE
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			rf.mu.Unlock()
		}

		// 在完成状态转变之前其它的工作协程都不能获得Image，所以这里加写锁

		// 重置 re.timer.C
		rf.timer.Stop()
		if len(rf.timer.C) > 0 {
			select {
			case <-rf.timer.C:
			default:
			}
		}

		// 下列操作中对server状态的读操作，都不会产生data-race
		// 因为对server状态的写操作在当前协程内

		switch rf.State {
		case FOLLOWER:
			ELT := electionTime()
			// 选举时间[200,450]ms
			rf.timer.Reset(ELT)
			Debug(dTimer, "[%d] S%d Convert FOLLOWER, ELT:%d", rf.CurrentTerm, rf.me, ELT.Milliseconds())
		case CANDIDATE:
			ELT := electionTime()
			rf.timer.Reset(ELT)
			Debug(dTimer, "[%d] S%d Convert CANDIDATE, ELT:%d", rf.CurrentTerm, rf.me, ELT.Milliseconds())

			rf.sendRequestVote(*rf.Image)
		case LEADER:

			// leader 任其开始时添加一个空的日志条目 no-op 条目
			// if rf.Log[len(rf.Log)-1].Term != rf.CurrentTerm {
			// 	index := len(rf.Log)
			//
			// 	rf.Log = append(rf.Log, Entry{
			// 		ApplyMsg: ApplyMsg{Command: nil, CommandIndex: index},
			// 		Term:     rf.CurrentTerm,
			// 	})
			// 	Debug(dAppend, "[%d] S%d Append Entry. IN:%d, TE:%d", rf.CurrentTerm, rf.me, index, rf.Log[index].Term)
			// }
			rf.timer.Reset(HEARTBEAT)
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
	rf.Image = &Image{
		update: make(chan func(*Image)),
		done:   make(chan signal),
		did:    make(chan signal),
		Raft:   rf,
	}
	rf.RWLog = &RWLog{Log: make([]Entry, 0)}
	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.timer = time.NewTimer(electionTime())

	// 创建用来提交日志的协程
	rf.commitCh = make(chan int)
	rf.commit()

	// start ticker goroutine to start elections
	Debug(dTest, "[%d] S%d Start.", rf.CurrentTerm, rf.me)
	go rf.ticker()

	return rf
}
