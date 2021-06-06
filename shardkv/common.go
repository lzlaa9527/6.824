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

	Cfgnum  int
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
	FromGID int
	ToGID   int
}

type PullReply struct {
	DB Database
	// 数据迁移前后，不光要同步数据库的数据；
	// 还要同步目前各个Clerk的Identifier，
	// 避免产生在reconfig前后在不同的备份组重复执行同一个请求
	SeqTable   map[int]int
	ReplyTable map[int]interface{}
	Err        Err
}

type MergeArgs struct {
	FromGID    int
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

type Tasks struct {
	mu     *sync.RWMutex
	tasks  map[int]TaskStruct
	Shards map[int]bool
}

type TaskStruct struct {
	Cfgnum    int
	ToGID     int
	ToServers []string
	Shards    []int
}

func NewTasks() Tasks {
	return Tasks{
		mu:     new(sync.RWMutex),
		tasks:  make(map[int]TaskStruct),
		Shards: make(map[int]bool),
	}
}

func (t Tasks) Add(ts TaskStruct) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.tasks[ts.ToGID] = ts
	for _, sid := range ts.Shards {
		t.Shards[sid] = true
	}
}

func (t Tasks) Remove(gid int) {
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

func (t Tasks) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.tasks)
}

func (t Tasks) Export() (tasks map[int]TaskStruct) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	tasks = make(map[int]TaskStruct)
	for gid, taskStruct := range t.tasks {
		tasks[gid] = taskStruct
	}
	return
}

func (t Tasks) Contains(shard int) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, existed := t.Shards[shard]
	return existed
}

func (t Tasks) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for gid := range t.tasks {
		delete(t.tasks, gid)
	}

	for sid := range t.Shards {
		delete(t.Shards, sid)
	}
}

