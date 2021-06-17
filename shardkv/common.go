package shardkv

import (
	. "6.824/common"
	"6.824/shardctrler"
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
	ShardsID []int
	Cfgnum   int
	FromGID  int
	ToGID    int
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

type ReConfigArgs struct {
	Config shardctrler.Config
	DB     Database
	SeqTable   map[int]int
	ReplyTable map[int]interface{}
}

type ReConfigReply struct {
	Err Err
}
