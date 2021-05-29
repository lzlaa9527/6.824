package kvraft

import "sync"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Kind  string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key     string
	ClerkID int
}

type GetReply struct {
	Err   Err
	Value string
}

type signal struct{}

// 记录每个Op，及其执行结果
// 但server执行完Op之后，关闭s以通知等待当前Op的Client
type OpResult struct {
	s     chan signal
	op    Op
	reply interface{}
}

type Index int

// 存储 raft Index处对应的Op，及其执行结果
type OpResults struct {
	// 如果table[index] != nil，说明存在clerk在等待index处的op
	table map[Index]*OpResult
	mu    *sync.Mutex
	done  chan signal
}

// 阻塞在i处的Op对应的signal channel上，并在被唤醒后获取到Op以及对应的Result
// 当Op被执行之后applier协程通过关闭该signal channel通知阻塞的工作协程
// 如果调用协程是第一个调用者，首先初始化该signal
func (or OpResults) Wait(i Index) *OpResult {
	or.mu.Lock()
	ret, ok := or.table[i]
	if ok == false {
		or.table[i] = &OpResult{
			s: make(chan signal),
		}
		ret = or.table[i]
	}
	or.mu.Unlock()
	select {
	case <-ret.s: // Op处理完毕
	case <-or.done: // Server宕机
	}
	return ret
}

// server 调用SetAndBroadcast唤醒所有等待i对应Op的工作协程(clerk)。
// 如果目前还没有clerk等待该Op，且wake为false则直接返回；
// 如果wake为true，说明存在clerk会等待该op的完成，因此需要创建并插入对应的OpResult，
// 并且closer(ret.s)告知等待的clerk，该op已经完成
func (or OpResults) SetAndBroadcast(i Index, op Op, re interface{}, wake bool) {
	or.mu.Lock()
	defer or.mu.Unlock()

	ret, ok := or.table[i]
	if !ok { // 没有等待该Op的工作协程，直接返回
		if !wake {
			return
		}
		or.table[i] = new(OpResult)
		ret = or.table[i]
	}

	ret.op = op
	ret.reply = re // 设置op的执行结果
	close(ret.s)   // 唤醒等待协程

}

func (or OpResults) Delete(i Index) {
	or.mu.Lock()
	defer or.mu.Unlock()

	delete(or.table, i)
}

func (or OpResults) Destory() {
	or.mu.Lock()
	defer or.mu.Unlock()
	close(or.done) // 通知所有等待的clerk返回
}

// 每一个Client的Op都有一个唯一标识符Identifier
// ClerkID	标识Client ID
// Seq		标识Client下一个Op的序号
type Identifier struct {
	ClerkID int
	Seq     int
}

// ITable是Server记录目前各个Client待提交Op的Seq的哈希表
// 关键字是Client的ClerkID
// 值是该Client目前待提交的Seq
type ITable map[int]int

// 返回clerkID对应Client的可用的Op标识符
func (itable ITable) GetIdentifier(clerkID int) Identifier {

	return Identifier{
		ClerkID: clerkID,
		Seq:     itable[clerkID],
	}
}

// 更新clerkID对应Client的下一个Op标识符
func (itable ITable) UpdateIdentifier(clerkID int) {
	itable[clerkID]++
}

// 如果i标识的Op已经被执行过了，Executed返回true
func (itable ITable) Executed(i Identifier) bool {
	return i.Seq < itable[i.ClerkID]
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ServerID int // 打包该Op的Server
	Kind     string
	Key      string
	Value    string
	ID       Identifier
}
