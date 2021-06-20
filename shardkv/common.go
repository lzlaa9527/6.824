package shardkv

import (
	. "6.824/common"
	"6.824/shardctrler"
	"sync"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Kind  string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Cfgnum  int // 请求对应的cfg编号
	ClerkID int
	OpSeq   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cfgnum  int
	ClerkID int
	OpSeq   int
}

type GetReply struct {
	Err   Err
	Value string
}

type PullArgs struct {
	Shards  []int
	Cfgnum  int
	FromGID int // 拉取数据的备份组gid
}

type PullReply struct {
	Err Err
	DB  Database

	// 数据迁移前后，不光要同步数据库的数据；
	// 还要同步目前各个Clerk的Identifier，
	// 避免产生：在reconfig前后在不同的备份组重复执行同一个请求。
	SeqTable   map[int]int
	ReplyTable map[int]interface{}
}

type MergeArgs struct {
	FromGID    int // 被拉取数据的备份组gid
	DB         Database
	SeqTable   map[int]int
	ReplyTable map[int]interface{}
}

type MergeReply struct {
	Err Err
}

type ReConfigArgs struct {
	Config shardctrler.Config
}

type ReConfigReply struct {
	Err Err
}

// 记录待执行的拉取数据的任务；
// 在reconfig之后，将需要拉取的任务记录在TaskStruck结构中；
// 当前config的拉取任务未完成之前，不能再次执行reconfig；
// 当数据操作请求到达时，需要判断被操作数据是否在拉去任务中(未完成数据迁移)，
// 如果在就不能执行操作，否则就可以。
//
// tasks中的TashStruct.Cfgnum和server.Config.Num相同。
//
// pullTasks使得server在迁移数据的同时向外提供服务。
type PullTasks struct {
	mu     *sync.RWMutex
	tasks  map[int]TaskStruct
	Shards map[int]bool // 正在数据迁移的shards
}

type TaskStruct struct {
	Cfgnum    int
	ToGID     int
	ToServers []string
	Shards    []int
}

func NewTasks() PullTasks {
	return PullTasks{
		mu:     new(sync.RWMutex),
		tasks:  make(map[int]TaskStruct),
		Shards: make(map[int]bool),
	}
}

func (t PullTasks) Add(ts TaskStruct) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.tasks[ts.ToGID] = ts
	for _, sid := range ts.Shards {
		t.Shards[sid] = true
	}
}

// 从gid标识的数据已经合并到数据库之中，可以删除对应的TaskStruct。
func (t PullTasks) Remove(gid int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.tasks[gid]; !ok {
		return
	}

	for _, sid := range t.tasks[gid].Shards {
		delete(t.Shards, sid)
	}
	delete(t.tasks, gid)
}

func (t PullTasks) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.tasks)
}

// PullTasks 需要作为server的状态进行持久化存储；
// 在拍摄快照时，申请读锁避免对map的并发访问。
func (t PullTasks) Export() (tasks map[int]TaskStruct) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	tasks = make(map[int]TaskStruct)
	for gid, taskStruct := range t.tasks {
		tasks[gid] = taskStruct
	}
	return
}

// 如果shard尚未迁移完成就返回true，否则返回false。
func (t PullTasks) Contains(shard int) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, existed := t.Shards[shard]
	return existed
}

func (t PullTasks) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for gid := range t.tasks {
		delete(t.tasks, gid)
	}

	for sid := range t.Shards {
		delete(t.Shards, sid)
	}
}
