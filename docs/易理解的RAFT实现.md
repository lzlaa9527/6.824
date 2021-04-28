# 易理解的RAFT实现——lab2

本文旨在讲述我的 raft 实现过程，本文所描述的系统设计降低了处理状态转化时的复杂性，易于理解。

## 设计概要

### Image记录server状态

raft 的难点之一在于正确处理并发情况下的状态转化，为了解决这个问题本文采用的方法是将所有的状态转化代码交由同一个协程执行，实现当中**`ticker`协程用来监听所有可能更新server状态的事件**，做出相应的处理。

raft 将所有的 server 划分为三类：Follower、Candidate、Leader 位于不同状态时具有不同的行为——创建工作协程处理RPC请求与响应，当 server 的状态发生改变时原来工作协程所做出的处理都应该是无效的，所以在响应/发出 RPC 之前都应该判断 server 的状态是否发生改变。如何判断 server 的状态是否改变？最简单的想法就是在工作协程创建伊始保存 server 的状态，紧接着执行相应的逻辑处理，在响应/发出 RPC 之前再查询 server 的状态进行比较，通过比较判断 server 状态是否发生改变；

基于上述思想，本文在实现中设计了保存有 server 状态的数据结构 `Image` ，**创建工作协程时获取当前状态下 server 的`Image`实例，在响应/发出 RPC 之前通过判断该`image`实例是否仍有效来判断 server 的状态是否发生更改。**

```go
// 所有的角色
const (
	FOLLOWER  int = iota
	CANDIDATE int = iota
	LEADER    int = iota
)

type Image struct {

	update chan func(i *Image)
	did    chan signal
	done chan signal

    // servre的状态
	CurrentTerm int
	State       int
	VotedFor    int
    
    // Image实例所属的server
	*Raft
}
```

`Image` 记录某一时刻 server 的状态；工作协程可以通过监听 `Image` 实例中的 `done` 管道判断 server 的状态是否改变，工作协程（在 `update` 函数中）通过关闭 `done` 管道以通知所有监听当前 `Image` 实例的工作协程放弃当前的任务；当工作协程需要更新 server 的状态，工作协程可以将 server 状态更新的方式通过 `Image` 实例中的`update`管道发送过去；`ticker` 协程通过监听 `update` 管道，来执行 `update` 函数更新server的状态；当 `ticker` 协程执行完 `update` 函数之后，向 `did` 管道发送信号通知等待的工作协程；

事实上每个`Image`实例均与`done`绑定，如果`done`被关闭也就表明与其绑定的`Image` 实例已失效；因此当一个`Image`实例失效时我们只需要替换`done`就好，不需要再重新创建一个`Image`实例。

`Image` 有如下两个方法：

+ `Done() bool`：如果当前 Image 实例已经失效，如果已失效表明 server 的状态已经发生改变则返回`true`，否则返回`false`。
+  `Update(act func(i *Image)) bool`：工作协程调用`Update`函数向与自己绑定的`Image`实例的`update` 管道传递更新 server 状态的函数 `act`；在发送之前需要先检查当前的`Image`示例是否已失效，则返回`false`；否则返回`true`。

**当调用`Done`或者`Update`返回`false`，就表示server得状态已经改变，当前的`Image`实例已经失效。**

```go
func (image Image) Update(act func(i *Image)) bool {

	// 判断image是否早已失效，1
	select {
	case <-image.done:
		return false
	default:
		break
	}

	select {
		// 判断image是否早已失效，2
	case <-image.done:
		return false
		
		// 发送更新server状态的函数，通知ticker协程更新server的状态
	case image.update <- act: 
		<-image.did	 // 发送成功后等待act函数执行完毕后返回
		return true
	}
}

// 工作协程通过判断done是否关闭，确定image是否已失效
func (image Image) Done() bool {
	select {
	case <-image.done: 
		return true
	default:
		return false
	}
}
```

在`Update`函数中有两个检查`image.done`是否关闭的逻辑，是因为`Update`函数需要同时监听两个管道，如果两个管道同时有数据流动那么`select`会随机选择一个分支执行，因此如果只有第二个`select`代码块，可能会发生意想不到的bug。

### ticker监听改变server状态的事件

![raft-图4](assets/raft-图4.png)

由Image的结构可以看出本文所指的描述server状态的字段由：`CurrentTerm、VotedFor、State`；

上图描述了server状态转换图，**可能**导致server状态转换的事件主要有

1. 计时器超时
2. 接收到Leader的的AE RPC(Append Entry RPC)请求，处理返回的AE RPC响应
3. 接收到Candidate的RV RPC(Request Vote RPC)请求，处理返回的RV RPC响应
4. server 宕机了

上述事件的发生并不一定会更新server的状态，比如Leader的心跳计时器超时就不需要更新server状态，只需要发送AE RPC即可；上文说到`ticker`协程用来监听所有可能更新server状态的事件，也即上面列出的3类事件。

本文通过`timer time.Timer`对象来模拟定时器，因此`ticker`协程首先要监听`timer.C`管道来确定是否发生了计时器超时事件；监听`dead`管道来确定server是否发生了宕机；对于AE RPC、RV RPC相关的事件发生时RPC会创建一个工作协程来处理，工作协程通过`Image.update`管道传递server状态的更新函数，因此`ticker`协程还需要监听`image.update`管道。

至此我们已经能够搭建起一个核心的框架了：

```go
func (rf *Raft) ticker() {

	for {
		select {
		case <-rf.dead:
			// 记得关闭协程，避免内存泄露
			return
		case f := <-rf.Image.update:
			// 执行工作协程创建的更新函数
		case <-rf.timer.C:
			// 如果是选举计时器超时就转为Candidate，并重置选举计时器
             // 如果心跳计时器超时，不需要改变状态
		}

        // 状态转变时进行持久化
		rf.persist()

		// 根据不同的状态执行后续动作
		switch rf.State {
		case FOLLOWER:
		case CANDIDATE:
			rf.sendRequestVote()
		case LEADER:
			rf.SendAppendEntries()
		}
	}
}
```

在`select`代码块中进行server状态的改变，**强调这里是唯一会改变server状态的地方**，然后在`switch case`代码块中根据不同的server状态执行不同的后续代码。



## Raft结构体

```go
type Raft struct {
   peers     []*labrpc.ClientEnd
   persister *Persister         
   me        int                 
   dead 	chan signal
   mu 		sync.RWMutex

    // Image、RWLog都必须是指针类型，因为部分工作协程会修改这两个属性的字段
    // 如果是值类型且没有额外操作的话，会丢失这些修改
   *Image	// server的状态: CurrentTerm、State、VotedFor
   *RWLog	
   timer 	   *time.Timer
   commitIndex int
   lastApplied int
    
   applyCh     chan ApplyMsg	// 用来传递可以提交的日志条目
   commitCh    chan int			// 通知applier协程提交日志条目

   nextIndex  []int
   matchIndex []int
}

// 支持并发操作的日志
type RWLog struct {
	mu            sync.RWMutex
	Log           []Entry   
}

type Entry struct {
	ApplyMsg
	Term  int	// 日志条目的Term，快照的SnapshotIndex
	Index int	// 日志条目的CommandIndex，快照的SnapshotIndex
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
	SnapshotValid bool
}
```

为了降低锁的粒度，raft中用的都是读写锁，无论是读写server状态还是读写日志，都是读多写少，因此使用读写锁可以提高性能。

## Raft的创建与持久化

测试代码通过调用`Make`来创建或者重新启动一个raft实例，如果是server宕机之后重新恢复server需要从`persister`对象中反序列化自己的`CurrentTerm、VotedFor、Log`字段；server需要在状态更新、添加日志和日志压缩后及时进行持久化。

### Make

```go
func Make(peers []*labrpc.ClientEnd, me int,
   persister *Persister, applyCh chan ApplyMsg) *Raft {
   rf := &Raft{}
   rf.peers = peers
   rf.persister = persister
   rf.me = me
   rf.applyCh = applyCh

   // Your initialization code here (2A, 2B, 2C).
   rf.dead = make(chan signal)
   rf.commitCh = make(chan int)
   rf.Image = &Image{
      update: make(chan func(*Image)),
      done:   make(chan signal),
      did:    make(chan signal),
      Raft:   rf,
   }
   rf.RWLog = &RWLog{Log: make([]Entry, 0)}
   rf.readPersist()
   rf.timer = time.NewTimer(electionTime())
   go rf.ticker()
   go rf.applier()
    
   Debug(dTest, "[%d] S%d START.", rf.CurrentTerm, rf.me)
   return rf
}
```

第19行会尝试从`readPersist`函数中读取自己持久化的状态。

### persist

```go
func (rf *Raft) persist() {
   rf.RWLog.mu.RLock()
   defer rf.RWLog.mu.RUnlock()

   w := new(bytes.Buffer)
   e := labgob.NewEncoder(w)

   e.Encode(rf.CurrentTerm)
   e.Encode(rf.VotedFor)
   e.Encode(rf.Log) // 这里包含日志条目与快照
   state := w.Bytes()

   rf.persister.SaveRaftState(state)
}
```

### readPersist

`readPersist`有几点需要注意的：

1. 如果之前没有持久化的数据，即raft是首次被创建，faft也应该正常被初始化；初始化值参考论文的Figure2。
2. 在raft首次被初始化时它的日志下标为0的地方有一个占位用的日志条目，设置其`Term=-1`永远不会被提交。
3. 在反序列化数据时，反序列化的字段顺序必须和序列化时的字段顺序一致。

```go
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
   }
}
```

## ticker改变server状态

根据*设计概要*中的分析`ticker`协程是整个实现中唯一改变server状态的协程；`ticker`监听到`server`宕机之后会关闭信号管道通知工作协程退出，避免协程因阻塞在管道中而发生内存泄漏；`ticker`监听到定时器超时之后，会更新server的状态，重置计时器以及发送RPC等；`ticker`监听到工作协程发来的`update`函数后，会执行`update`函数更新server的状态，并根据server的当前状态执行不同的逻辑。

### Kill & killed

```go
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
```

通过关闭`rf.dead`管道通知所有的工作协程`raft`宕机了，是一种简单有效的实现方式。

### timer

Raft 结构体中定一个了个`timer *time.Timer`成员用来模拟定时器，定时器超时后runtime系统会向`timer.C`管道中发送一个信号，`ticker`协程监听到信号之后只要server没有宕机就需要重置定时器；如果server是Follower、Candidate就需要随机设置一个选举时间，如果server是Leader就将倒计时设置为心跳时间

```go
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
```

1. 心跳间隔很重要。测试要求1S内发送不超过10次心跳，所以`HEARTBEAT`最小是100ms；增大HEARTBEAT会降低发送心跳的频率，一般情况下不超过选举时间没问题；然而当网络不稳定时心跳有可能会丢失在网络中，极端情况下一连数个心跳因网络问题没有发送到Follower，这可能导致Follower发起一轮选举。

2. 选举时间的范围也很重要。类似的问题，首先测试要求最长5s内要完成leader选举，因此选举时间不易过大；其次如果选举时间过小，当心跳丢失在网络中时Follower极有可能发起一轮选举；然后，为了在不稳定的网络环境中尽可能避免瓜分选票的发生，每个server的选举时间不能够太集中，也就是随机范围尽可能大；

   如果没有设置好心跳间隔和选举时间的范围，在`TestFigure8Unreliable2C`中极端不稳定的网络环境下可能产生**活锁**问题，即长时间无法选出`Leader`。

   经过实验要想顺利通过这个测试令心跳间隔为100ms，选举时间范围设为[300ms,600ms]是可行的。

3. 在关闭定时器之后，要清空`timer.C`中可能存在的值。

### ticker

关于工作协程如何处理RPC请求、响应将在对应的Leader选举，日志复制章节内讲解；目前只需知道工作协程通过向与server绑定的`Image`实例中的`Image.update`管道中传递`update`函数，来通知`ticker`协程更新server状态即可。至此已经介绍完了`ticker`需要监听的所有事件，下面给出的是`ticker`协程的完整实现：

```go
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
					rf.nextIndex[i] = len(rf.Log)
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
		case FOLLOWER: // FOLLOWER等待选举计时器超时
		case CANDIDATE:
			rf.sendRequestVote()
		case LEADER:
			rf.SendAppendEntries()
		}
	}
}
```

`ticker`执行在一个`for`循环中，除非server宕机否则永远不会停止；ticker协程就做三件事更新server状态，重置计时器，根据server的状态执行后续动作。

这里再次重申**`rf.mu.Lock()`与`rf.mu.Unlock()`之间是整个设计中唯一修改server状态的代码**，当在工作协程中读取server的状态时首先要申请`rf.mu`读锁，如果工作协程获得了读锁`ticker`协程对server状态更新的操作就会阻塞在`rf.mu`的写锁上。

接下来的任务就是完成Leader选举，日志复制。

## Leader选举

### RequestVoteArgs & RequestVoteReply

```go
type RequestVoteArgs struct {
   Term         int
   CandidateId  int
   LastLogIndex int
   LastLogTerm  int
}

type RequestVoteReply struct {
   Term        int
   VoteGranted bool
   ID          int  // 投票人ID，用作日志处理
   Valid       bool // 当RPC失败或者投票者在处理RV PRC过程中状态发生改变，置Valid为false
}
```

较Figure 2所述，`RequestVoteReply`多了两个字段`ID`、`Valid`，其中`Valid`简化了对RV RPC的处理。一个工作协程如果从收到RV RPC开始到发送RPC响应之间，其状态发生改变那么该工作协程就不能再执行下去了并将此时的`reply.Valid`置为`false`。对于`votesCounter`协程，会将`reply.Valid=false`的选票视为单纯的反对票。

### sendRequestVote

当选举计时器超时之后，Candidate会调用`sendRequestVote`向其它的peers发送RV(Request Vote) RPC请求，当收到RV RPC的响应之后将`RequestVoteReply`通知`votesCounter`协程开始计票；具体得`sendRequestVote`只用处理三件事：

1. 准备参数
2. 创建计票协程
3. 向其它的peers发送RV RPC，并将响应结果发送到计票协程中

在`VotesCounter`协程中为了保证计票结果得正确性，在获得半数赞成票之前`votesCounter`协程需要统计所有的票才能退出，所以无论是否为有效票都要将`RequestVoteReply`发送给`votesCounter`；另一点需要注意的是，server 只要获得绝大多数peers的赞成票就可以宣布当选为Leader，因此当server的状态转变之后可以提前宣布选举结束，以减少RPC的数量。

```go
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
	Debug(dVote, "[%d] S%d SEND RV RPC", image.CurrentTerm, rf.me)
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
```

### votesCounter

如何统一的处理一轮选举所有的选票呢，简化并发问题最简单有效的方法就是将其串行化。具体实现就是将所有选票结果通过管道交给`votesCounter`协程，`votesCounter`协程逐个处理。这里需要强调的是`votesCounter`协程必须在发送RV RPC之前运行，因为协程的调度是不可控的，如果不做这个限制，`votesCounter`协程可能会错失掉部分选票结果。

处理过程中，对于无效票计票协程只增加总票数即可，对于反对票如果过反对票的`term>currentTerm`server需要将自己转换为Follower并重置选举计时器；如果赞成票的数目达到其它peers数目的一半，server就可以转化为Leader。

正如*设计概要*中所言，在处理过程中我们必须判断server的状态是否已改变，若是就没必要再执行下去了；但是在`votesCounter`协程中不能简单的退出，因为其它的选票结果人会通过`replyCh`发送过来，如果直接退出会导致发送协程的阻塞。

下面给出`votesCounter`的实现：

```go
// 用来计票的工作协程
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
				goto check	// 判断是否已经收到所有的选票
			}

			// 处理有效票
			if reply.Valid {
				Debug(dVote, "[%d] S%d <-REPLY S%d, V:%v GV:%v T:%d", image.CurrentTerm, image.me, reply.ID, reply.Valid, reply.VoteGranted, reply.Term)
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
							Debug(dTimer, "[%d] S%d CONVERT FOLLOWER <- S%d NEW TERM.", i.CurrentTerm, i.me, reply.ID)
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
						// 设置新的Image
						i.State = LEADER

						// 因为需要改变server的状态，所以应该使之前的Image实例失效
						close(i.done)
						// 重新绑定一个image.don就能使新Image实例生效
						i.done = make(chan signal)
						Debug(dVote, "[%d] S%d CONVERT LEADER.", i.CurrentTerm, i.me)

						// 重置计时器，设置心跳时间
						i.resetTimer()
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
```

至此我们看到了如何使用`Image`实例来简单的判断server的状态是否发生改变。

### RequestVote

每当server收到RV RPC请求后都会创建`RequestVote`协程处理RPC请求，检查选举条件。在处理过程中server的状态可能会发生改变，处理这种情况的一个基本原则：**如果创建协程时刻RPC参数不满足选举条件应直接返回投反对票，但是创建协程时RPC请求参数满足选举条件并不能直接投赞成票，因为在投票之前server的状态可能已经转变了，此时的选票应该是无效的。**在实现过程中，能看到`Image`数据结构带来的便利性。

根据论文可知，`RequestVote`做如下检查：

1. 如果Candidate的`term`已经过时了，立即返回`false`。

2. 如果Candidate的`term`比较新，server应该转换为Follower；但是不应该重置定时器，论文中提到server在确保投票后才会重置定时器。根据《students’ guide》的描述，此时如果重置定时器的话，会导致很多不满足选举条件的server发起选举从而导致具有选举条件的server无法完成选举；换句话说，如果随意地重置定时器。

3. 如果满足限制选举条件就投赞成票；否则投反对票。

   限制选举条件：

   + 如果Candidate的最后的日志条目Term更大，投赞成票
   + 如果Term一样大，Candidate的日志更长或者和server的日志一样长，投赞成票

```go
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
   reply.Term = max(term, currentTerm)
   reply.Valid = true
   Debug(dVote, "[%d] S%d RECEIVE<- S%d,T:%d LLI:%d LLT:%d", currentTerm, me, candidateID, term, args.LastLogIndex, args.LastLogTerm)

   // CANDIDATE的term过时了，投反对票
   if term < currentTerm {
      reply.VoteGranted = false // 通知CANDIATE更新自己的term
      Debug(dVote, "[%d] S%d REFUSE -> S%d, OLD TERM", currentTerm, me, candidateID)
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
         Debug(dVote, "[%d] S%d CONVERT FOLLOWER <- S%d, NEW TERM", term, rf.me, candidateID)
      })
      // server状态已经发生了改变
      if !reply.Valid {
         return
      }

   }

   // 已经没有票了
   if votedFor != -1 && votedFor != candidateID {
      reply.VoteGranted = false
      Debug(dVote, "[%d] S%d REFUSE -> S%d, NO VOTE.", currentTerm, me, candidateID)
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
         Debug(dVote, "[%d] S%d VOTE -> S%d", currentTerm, me, candidateID)

         i.resetTimer() // 确定投赞成票后要重置计时器
      })
      return
   }
   // 拒绝投票
   reply.VoteGranted = false
   Debug(dVote, "[%d] S%d REFUSE VOTE -> S%d, Old LOG", currentTerm, me, candidateID)
}
```

## 日志复制

在开始之前要先做一点说明，论文中将日志条目的添加与心跳的发送划分为两个事件。但是具体实现时可以将其视为同一个事件，Leader周期性地发送心跳，如果需要向peer发送日志就在心跳包中添加日志条目即可，并且peers在收到AE RPC时对于两类事件的处理也是统一的。行为的统一能够大大降低实现的复杂度。（在日志压缩部分，可以看到日志的也可以包含在心跳包中）

### start

`start`接受客户端的请求，将请求打包成日志条目添加到日志中并最终同步到每一个server中。只有Leader能够接受客户端的请求，如果一个客户端通过`start`将请求发送给一个非Leader的server，`start`应立即返回:

```go
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
		ApplyMsg: ApplyMsg{Command: command, CommandIndex: index, CommandValid: true},
		Term:     rf.CurrentTerm,
		Index:    index,
	})

	Debug(dClient, "[%d] S%d APPEND ENTRY. IN:%d, TE:%d， CO:%v", rf.CurrentTerm, rf.me, index, rf.Log[index].Term, command)

	return index, term, isLeader
}
```

### AppendEntriesArgs & AppendEntriesReply

```go
type AppendEntriesArgs struct {
   Term         int // leader's CurrentTerm
   LeaderId     int // so follower can redirect clients
   LeaderCommit int // leader's commitIndex

   PrevLogIndex int // index of Log entry immediately preceding new ones
   PrevLogTerm  int // CurrentTerm of prevLogIndex entry
   Log          []Entry // log entries
}

type AppendEntriesReply struct {
   Valid bool 	// Valid=true表示有效响应，否则表示无效响应

   Term    int
   Success bool

    // 为了加快日志的back up，《students' guide》的优化
   ConflictIndex int	
   ConflictTerm  int
}
```

当Leader的心跳定时器到期之后会周期性地发送心跳(可能包含日志，也可能不包含)给其它的peers，为了实现日志的一致性检查，需要在`AppendEntriesArgs`中设置`PrevLogIndex`，`PrevLogTerm`字段，以及为了通知peers提交已同步的日志可要设置`leaderCommit`字段；

### caculateCommitIndex

`AppendEntriesArgs.leaderCommit`字段的内容即为`Raft`结构体中的`commitIndex`字段，`commitIndex`字段的计算可以发生在Leader接到peers的RPC Reply时，也可以发生在发送心跳之前，本文采用的是在发送心跳之前周期地计算`CommitIndex`。

`caculateCommitIndex`在计算出新的`commitIndex`之后如果不大于原来的值就没必要在更新了；`commitIndex`更新之后不能直接通知applier协程去提交日志，**还要检查要提交的日志中是否有当前任期的日志条目**。

```go
func (rf *Raft) calculateCommitIndex() {

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

   // 没必要更新commitIndex字段
   if newCommitIndex <= rf.commitIndex {
      return
   }

   // 不能提交其它任期的entry
   rf.RWLog.mu.RLock()
   if rf.Log[newCommitIndex].Term != rf.CurrentTerm {
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
```

这里有一处对于`matchIndex`的读写冲突，在处理AE(AppendEntey) RPC的响应时可能会并发地更新`matchIndex`，但是这并不影响正确性：在server状态不变的前提下`commitIndex`是单调增长的，即时发生了读写冲突也仅仅是延缓了日志提交的时间，但绝不会重复提交日志；另一方面Leader绝不会删除或修改自己的日志。综上对`matchIndex`的读写冲突不会影响正确性。

可能有读者觉得`commitIndex`也可能存在读写冲突，事实上并没有。Leader只会在`calculateCommitIndex`函数内更新`commitIndex`，而`calculateCommitIndex`在ticker协程内被调用，它是串行执行的；另外一处更新`commitIndex`的地方是在收到AE RPC添加了日志之后，但是添加日志之前需要修改状态，修改状态的代码也发生在`ticker`协程中，也就是说Leader不可能同时执行`calculateCommitIndex`和更新自己的状态。综上所述，`commitIndex`的更新不存在读写冲突。

### SendAppendEntries

更新完`commitIndex`之后，就可以准备参数向其它peers发送AE RPC了，注意RPC的发送以及响应应该在协程内执行避免延迟对其它peers的RPC发送。

```go
func (rf *Raft) SendAppendEntries() {
	image := *rf.Image        // 获取此时的Image实例
	rf.calculateCommitIndex() // 更新commitIndex
	Debug(dAppend, "[%d] S%d SEND AE RPC.", rf.CurrentTerm, rf.me)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		// 记录下此时的nextIndex,matchIndex；
		// 在收到RPC响应之后如果nextIndex,matchIndex发生改变，便无需更新nextIndex,matchIndex
		var (
			nextIndex  = rf.nextIndex[server]
			matchIndex = rf.matchIndex[server]
		)

		// 每一peer都有一个独立的AppendEntriesArgs实例
		args := &AppendEntriesArgs{
			Term:         image.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		rf.RWLog.mu.RLock()
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.Log[prevLogIndex].Term

		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = prevLogTerm
		args.Log = append(rf.Log[:0:0], rf.Log[nextIndex:]...) // 深拷贝

		rf.RWLog.mu.RUnlock()
		go aerpc(image, server, nextIndex, matchIndex, args)
	}
}
```

这里唯一需要讲解的是在发送AE RPC之前记录下来了每个peer此时的`matchIndex`、`nextIndex`，至于为什么这么做会在`aerpc`中讲解。

### aerpc

`aerpc`向标号为server的peer发送AE RPC，并处理后续响应。在接收到RPC响应之后，aerpc需要做以下检查：

1. 收到RPC响应之后应立即检查当前的`Image`实例是否有效，以及响应是否有效；如果`Image`实例失效，或者响应无效立即退出。

2. 如果peer的`term`大于Leader的`currentTerm`，Leade应该转化为Follower但不要重置定时器。

3. 如果响应有效且`reply.Success=false`，就需要**快速确定**`nextIndex`的值。这里采用了一些优化，以便能够通过`TestBackup2B`。

   + 如果`reply.ConflictTerm == -1`，表明peer的日志较短，直接令`newNextIndex = reply.ConflictIndex`。

   + 否则，就从Leader的日志中**从右向左**找到首个`index`，使得`Log[index].Term == reply.ConflictTerm`，即对于Leader`index`点之前的日志与`peer`的日志相匹配；为了避免不必要的搜索，在搜索之前应该先判断Leader的日志中是否存在这样的日志条目。

     首先要明确`index>=reply.ConflictIndex`，如果找到的`index<reply.ConflictIndex`是没有意义的，因为此处的日志条目和peer中**相同的位置**的日志条目仍然是冲突的。因为相同`Term`的日志条目时连续的，且`reply.ConflictIndex`是peer日志中首个`Term==reply.ConflictTerm`的日志条目，所以`index<reply.ConflictIndex`处的日志条目肯定是冲突的。

     因此在搜索前我们先判断Leader中`reply.ConflictIndex`处日志条目的Term是否等于`reply.ConflictTerm`，如果不等就没必要搜索了；如果相等就从`reply.ConflictIndex`处向左搜索整个日志，找到该`Term`下提交的最后一个日志条目；

   + 因为没有通过日志匹配的检查，所以不应该更新`matchIndex`。

4. 如果响应有效且`reply.Success=true`直接计算`newNextIndex、newMatchIndex`的值即可。

5. 如果发送的是不含日志条目的心跳，就不用更新`nextIndex`，`matchIndex`的值。

6. 可能存在多个协程同时更新同一个peer的`nextIndex、matchIndex`这会产生读写冲突，加锁是最直接的解决办法，但是会降低性能。本文采用的CAS的方法来解决该问题，在发送RPC之前记录此时的`nextIndex、matchIndex`值，更新时如果`nextIndex、matchIndex`值已经改变就不应该再更新；这样可以避免覆盖掉其他`aerpc`协程的更新，也能保证安全性。

下面给出`aerpc`的实现：

```go
// Aerpc 向标号为peerIndex的peer发送AE RPC，并处理后续响应
// image      发送RPC时server的Image实例
// nextIndex   发送RPC时peer的nextIndex值
// matchIndex  发送RPC时peer的matchIndex值
// args          RPC参数
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
      image.RWLog.mu.RLock()	// 避免对日志的读写冲突
      newNextIndex = reply.ConflictIndex
       
      // 如果reply.ConflictTerm != -1从日志中搜索和FOLLOWER可以匹配的日志条目
      if reply.ConflictTerm != -1 {
         // 如果image.Log[newNextIndex].Term != reply.ConflictTerm，
         // 那么newNextIndex左边的日志也不可能与FOLLOWER的日志匹配
         if image.Log[newNextIndex].Term == reply.ConflictTerm {
            for ; newNextIndex < args.PrevLogIndex && image.Log[newNextIndex].Term == reply.ConflictTerm; newNextIndex++ {
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
```

### AppendEntries

`AppendEntries`用来处理AE RPC请求，**同`RequestVote`在接收到RPC请求时就要创建`Image`的实例，如果检查通过在响应RPC前应该判断当前的Image实例是否有效，如果失效置`reply.Valid=false`**。

`AppendEntries`主要执行如下操作：

1. 如果Leader的`Term`过时了，立即返回。
2. 否则当前的server应该转化为该Leader的Follower并重置自己的选举定时器；这里需要注意的是：如果当前的server一直是该Leader的Follower就不需要改变状态，否则就应该改变状态并使得之前的`Image`实例失效。
3. 紧接着执行日志的一致性检查，再次强调如果检查不通过可以直接返回`false`，但是检查通过不能直接返回`true`，因为server的状态已经发生改变的话，检查时所用的`Image`实例就是过时的。
4. 检查通过之后要添加日志，在添加日志中不光要申请日志的写锁避免产生对日志的读写冲突，还要保证server的状态不会发生改变，这个过程中申请server的读锁。
5. 日志添加完成之后响应RPC之前要进行持久化操作。
6. 最后可能需要更新`commitIndex`。为了保证并发修改`commitIndex`的正确性，这里同样采用CAS的方式。

具体的一致性检查内容，和日志添加细节参考论文的Figure 2和课程的《students’ guide》

```go
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

	// 避免对rf.Log的读写冲突，但并不能保证server的状态不会发生改变
	rf.RWLog.mu.RLock()
	var (
		prevLogIndex = args.PrevLogIndex
		prevLogTerm  = args.PrevLogTerm
	)

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

	// RWLog[prevLogIndex].Term != prevLogTerm找到ConflictIndex然后返回false
	if prevLogTerm != rf.Log[prevLogIndex].Term {
		reply.Success = false
		reply.ConflictTerm = rf.Log[prevLogIndex].Term

		// 找到首个term为ConflictTerm的ConflictIndex
		for reply.ConflictIndex = prevLogIndex; rf.Log[reply.ConflictIndex].Term == reply.ConflictTerm; reply.ConflictIndex-- {
		}

		reply.ConflictIndex++

		rf.RWLog.mu.RUnlock()
		Debug(dAppend, "[%d] S%d REFUSE <- S%d, CONFLICT", currentTerm, me, leaderID)

		// 如果server状态已经改变，上述数据是无效的
		reply.Valid = !image.Done()
		return
	}
	rf.RWLog.mu.RUnlock()

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
	// 这里不需要加锁，因为只要不减小commitIndex都是安全的;
	// (减小commitIndex，可能让一条日志条目提交两次)
	defer func() {

		var (
			leaderCommit   = args.LeaderCommit
			newCommitIndex int
		)

		newCommitIndex = min(leaderCommit, args.PrevLogIndex+len(args.Log))

		// 判断commitIndex是否需要更新
		var commitIndex int
		commitIndex = int(atomic.LoadInt64((*int64)(unsafe.Pointer(&rf.commitIndex))))

		// 因为申请了读锁，server的状态不会发生改变
		// 只有在commitIndex < newCommitIndex时才更新；使用CAS是为了保证并发安全
		if commitIndex < newCommitIndex && atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(&rf.commitIndex)), int64(commitIndex), int64(newCommitIndex)) {

			// 一边去通知commit协程提交日志
			go func() {
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

	// i，j作为指针扫描FOLLOWER与LEADER的日志，进行日志匹配条件的检查
	i, j := args.PrevLogIndex+1, 0 // AE RPC中不含快照

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
```

### applier

`applier`协程用来按顺序提交所有已经同步的日志条目，在之前的代码中可以看到，只要server的`commitIndex`更新之后就应该将新的`commitIndex`通过`commitCh`传递给`applier`协程。applier协程不能重复提交同一个日志条目，因此每一个有效的`commitIndex`都不应该小于`lastApplied`，并且每提交一个日志条目都要增加`lastApplied`的值。

```go
func (rf *Raft) applier() {
	for commitIndex := range rf.commitCh {
		if rf.killed() {
			return
		}

		// rf.lastApplied 是递增的所以不会重复执行同一个日志条目
		for commitIndex >= rf.lastApplied {
			Debug(dCommit, "[%d] S%d APPLY LA:%d", rf.CurrentTerm, rf.me, rf.lastApplied)
			rf.applyCh <- rf.Log[rf.lastApplied].ApplyMsg
			rf.lastApplied++
		}
	}
}
```

**注意到这里读取日志时并没有申请日志的读锁，这是因为`raft`保证不会修改、删除可以提交的日志条目！**即使有数据竞争也是正确的。

到此为止，关于Lab 2A，2B，2C的实验内容已经介绍完毕。

完整代码见：[lzlaa/6.824 at lab-2A-2B-2C (github.com)](https://github.com/lzlaa/6.824/tree/lab-2A-2B-2C)

## Debug

代码中用的Debug函数来自于[实验材料](https://blog.josejg.com/debugging-pretty/)，使用方式详见链接；本文做了简单的修改，具体内容如下：

```go
type logTopic string

const (
	dClient  logTopic = "CLNT" // 客户端请求
	dCommit  logTopic = "CMIT" // 提交日志
	dKill    logTopic = "KILL" // server宕机
	dAppend  logTopic = "APET" // AE RPC
	dPersist logTopic = "PERS" // 持久化操作
	dTimer   logTopic = "TIMR" // 定时器操作
	dVote    logTopic = "VOTE" // RV RPC
	dSnap    logTopic = "SNAP" // 快照
	dTerm    logTopic = "TERM" // 修改任期
	dTest    logTopic = "TEST" // 测试信息
	dTrace   logTopic = "TRCE"
	dError   logTopic = "ERRO"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debug = 1

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("DEBUG") // DEBUG = 1打印输出，否则不打印
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugStart = time.Now()
	debug = getVerbosity()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)

	}
}
```

## Snapshot

lab2中最后一关是通过快照技术实现日志压缩，一旦在某个时刻创建了一个快照之后之前时刻的所有日志都可以被删除；这一举措基本会影响到所有的日志读写操作，原先的日志条目中`commandIndex`就相当于其索引，但是引入快照技术之后该条件不再成立。因此要实现snapshot需要对前面的实现来一个大改造。

实验材料中关于snapshot中只给了如下的两个相关的接口，并没有给出InstallSnapshot RPC有关的接口。这表明我们可以不单独实现一个InstallSnapshot RPC接口就能够完成lab 2D。

```go
Snapshot(index int, snapshot []byte) 
CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte)
```

很明显我们只能通过AE RPC来传递snapshot，回头再来看ApplyMsg的结构，发觉该思路完全可行。

```go
type ApplyMsg struct {
   CommandValid bool	// true，该msg是一个日志条目
   Command      interface{}
   CommandIndex int

   // For 2D:
   Snapshot      []byte
   SnapshotTerm  int
   SnapshotIndex int
   SnapshotValid bool	// true，该msg是一个snapshot
}
```

在开始实现之前需要明确各个接口的交互是怎样的：

1. 正常情况下，Follower与Leader的日志保持同步，但日志达到一定长度后测试代码会调用`Snapshot`要求`raft`删除`index`之前（包括）的日志，并且将日志、快照以及server的状态持久化保存；

2. 在不稳定网络环境中，一个Follower可能落后Leader较多的日志，Leader首先需要找到与其匹配的日志条目并将该点以及以后的日志条目均发送过去。但是在搜索过程中，可能发现匹配的日志条目已经被删除加入到快照中了，此时Leader只需要将快照及其后续日志条目通过AE RPC发送过去即可。

   Follower在接收到快照之后，首先要检查是否过期，即该RPC由于网络延迟已经无效了，如果未过期就会接受该快照。接受快照之前需要将`LastIncludeIndex`点之前的日志条目删除，然后将该快照及其后续日志插入到Follower的日志中，在插入过程中仍需要执行一致性检查。

   此时快照存储在日志中，但只有快照被提交之后才会被应用。快照和日志条目一样在`applier`协程中被提交，提交之后测试代码会调用`CondInstallSnapshot`要求raft判定是否安装该快照过时的快照不应该被安装，安装快照时并不需要再次压缩日志。

基于上述分析明确了各个接口的功能，接下来就是设计数据结构实现接口。

+ 根据快照的性质：一旦建立了一份快照，该快照所包含的日志条目都可以被删除；那么很自然的，我们可以将快照视为日志中的第一项。
+ 另一个问题是，实现日志压缩之后就不能直接通过`commandIndex`来索引日志条目了，可以**通过日志条目相对于快照条目的偏移量来索引**。

根据如上分析可以在原来日志数据结构的基础上扩展新的数据结构：

```go
type RWLog struct {
	mu            sync.RWMutex
	Log           []Entry // 如果当前的server有快照，那么快照一定是第一个日志条目
	SnapshotIndex int     // 当前快照的LastIncludeIndex，所有日志条目索引的基准
}
```

事实上只增加了一个`SnapshotIndex`字段用来表示当前快照的`LastIncludeIndex`，至此可以通过`Log[commandIndex - SnapshotIndex]`来访问日志条目。

如前所述引入快照之后带来的影响就是建立了日志条目相对索引和绝对索引的映射关系：`rf.Log[index].Index=index+rf.RWLog.SnapshotIndex`。对于之前实现的具体修改这里不再赘述读者可以参看[仓库](https://github.com/lzlaa/6.824)的代码，所有涉及到`Log`读取的操作都需要修改。

需要强调的是在AE RPC的参数中设置了一个布尔类型的`HasSnapshot`字段，如果发送的RPC中包含快照该字段置为`true`，否则置为`false`。在处理AE RPC请求的`AppendEntries`中根据是否含有快照做些不同的处理，比如一旦接受了快照就要删除哪些不需要的日志条目，否则只有追加日志条目。（注意，为了避免内存泄漏应该采用深拷贝的方式来复制日志，而不应该使用切片操作）

### Snapshot

但日志达到一定长度之后测试代码会调用`Snapshot`要求`raft`压缩日志：

```go
// Snapshot 更新rf.RWLog.SnapshotIndex、删除过时的日志并生成快照、持久化日志和快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	// 防止rf.RWLog.SnapshotIndex的读写冲突
	rf.RWLog.mu.Lock()
	Debug(dSnap, "[%d] S%d CALL SNAPSHOT, LII:%d, SI:%d", rf.CurrentTerm, rf.me, index, rf.RWLog.SnapshotIndex)

	offset := index - rf.RWLog.SnapshotIndex

	// 丢弃过时的快照
	if offset < 0 {
		Debug(dSnap, "[%d] S%d REFUSE SNAPSHOT - OLD INDEX, LII:%d, SI:%d", rf.CurrentTerm, rf.me, index, rf.RWLog.SnapshotIndex)
		rf.RWLog.mu.Unlock()
		return
	}
	rf.RWLog.SnapshotIndex = index

	// 删除原来的日志条目
	entries := make([]Entry, len(rf.Log[offset:]))
	copy(entries, rf.Log[offset:])
	rf.Log = entries

	// 首个日志条目用来存储snapshot
	rf.Log[0] = Entry{
		ApplyMsg: ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  entries[0].Term,
			SnapshotIndex: entries[0].Index,
		},
		Term:  entries[0].Term,
		Index: entries[0].Index,
	}
	rf.RWLog.mu.Unlock()

	// 持久化日志和快照
	rf.mu.RLock()
	rf.persist()
	rf.mu.RUnlock()
}
```

### CondInstallSnapshot

```go
// CondInstallSnapshot 如果lastIncludedIndex < rf.RWLog.SnapshotIndex，
// 则该快照已经过时立即返回false；否则返回true。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.RWLog.mu.RLock()
	defer rf.RWLog.mu.RUnlock()

	// 不安装老旧的快照
	if lastIncludedIndex < rf.RWLog.SnapshotIndex {
		Debug(dSnap, "[%d] S%d REFUSE INSTALL - OLD SNAP, LII:%d, SI:%d", rf.CurrentTerm, rf.me, lastIncludedIndex, rf.RWLog.SnapshotIndex)
		return false
	}
	Debug(dSnap, "[%d] S%d INSTALL SNAPSHOT, LII:%d, SI:%d", rf.CurrentTerm, rf.me, lastIncludedIndex, rf.RWLog.SnapshotIndex)
	return true
}
```



## 如何测试

要求python3的环境，测试脚本是实验材料中给的python脚本。

```sh
cd 6.824/raft
# -v 打印输出信息
# -p 最大并行数
# -n 测试次数
# -o 错误结果保存目录
# [Tests]指定的测试函数列表
python3 dstest.py -v  -p 20 -n 100 -o ./output [Tests]
```

查看错误日志

```sh
cd 6.824/raft
# -c 输出的列数，即测试中servers的数目
python3 dslog.py output/xxx.log -c 3
```

dstest.py，dslog.py具体使用方式见：[Debugging by Pretty Printing (josejg.com)](https://blog.josejg.com/debugging-pretty/)，本文做了少量修改，但使用方式未变。

