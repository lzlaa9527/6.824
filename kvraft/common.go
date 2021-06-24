package kvraft

import . "6.824/common"

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Kind  string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID int
	OpSeq   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key     string
	ClerkID int
	OpSeq   int
}

type GetReply struct {
	Err   Err
	Value string
}