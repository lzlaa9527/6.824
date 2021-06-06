package shardctrler

import . "6.824/common"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shards, gid) -- hand off one shards from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shards. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shards -> gid
	Groups map[int][]string // gid -> servers[]
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	ClerkID int
	OpSeq   int
}

type JoinReply struct {
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	ClerkID int
	OpSeq   int
}

type LeaveReply struct {
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClerkID int
	OpSeq   int
}

type MoveReply struct {
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	ClerkID int
	OpSeq   int
}

type QueryReply struct {
	Err         Err
	Config      Config
}
