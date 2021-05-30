package kvraft

import "6.824/labrpc"

var ClerkID int

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int // 目前所知的leader ID
	ClerkID  int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderID = 0

	ck.ClerkID = ClerkID
	ClerkID++

	Debug(dClient, "[*] C%d init.", ck.ClerkID)
	return ck
}

func (ck *Clerk) Get(key string) string {

	arg := &GetArgs{
		Key:     key,
		ClerkID: ck.ClerkID,
	}

	for {
		Debug(dClient, "[*] C%d SEND GET_REQ TO S%d", ck.ClerkID, ck.leaderID)
		reply := &GetReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", arg, reply)
		if !ok {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		Debug(dClient, "[*] C%d RECEIVE REPLY:%+v", ck.ClerkID, reply)
		switch reply.Err {
		case OK:
			return reply.Value
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	arg := &PutAppendArgs{
		ClerkID: ck.ClerkID,
		Key:     key,
		Value:   value,
		Kind:    op,
	}

	for {
		Debug(dClient, "[*] C%d SEND PA_REQ TO S%d", ck.ClerkID, ck.leaderID)
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", arg, reply)
		if !ok {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		Debug(dClient, "[*] C%d PA REPLY:%+v", ck.ClerkID, reply)
		switch reply.Err {
		case OK:
			return
		case ErrWrongLeader:
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
