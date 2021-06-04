package kvraft

import (
	. "6.824/common"
	"6.824/labrpc"
	"6.824/raft"
	"time"
)

var ClerkID int

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int // 目前所知的leader ID
	ClerkID  int
	OpSeq    int // clerk下一个Op使用的sequence number
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderID = 0

	ck.ClerkID = ClerkID
	ClerkID++

	Debug(DClient, "[*] C%d init.", ck.ClerkID)
	return ck
}

func (ck *Clerk) Get(key string) string {

	arg := &GetArgs{
		Key:     key,
		ClerkID: ck.ClerkID,
		OpSeq:   ck.OpSeq,
	}
	ck.OpSeq++

	time.Sleep(1 * time.Millisecond)

	co := 0
	for {

		Debug(DClient, "[*] C%d SEND GET_REQ TO S%d", ck.ClerkID, ck.leaderID)
		reply := &GetReply{}

		retCh := make(chan bool, 1) // 这里必须是带缓冲的，为了能够让工作协程顺利退出
		go func() {
			retCh <- ck.servers[ck.leaderID].Call("KVServer.Get", arg, reply)
		}()

		var ok bool
		select {
		case ok = <-retCh:
		case <-time.After(raft.HEARTBEAT * 10): // 请求超时
			ok = false
		}

		// Call返回false或者定时器到期，表明请求超时
		if !ok {
			Debug(DClient, "[*] C%d RPC:%d TIMEOUT", ck.ClerkID, ck.OpSeq-1)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			co++
		} else {
			Debug(DClient, "[*] C%d GET REPLY:%+v", ck.ClerkID, reply)
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
				ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
				co++
			}
		}

		// 如果所有的server都不是leader，那就等待300ms
		if (co+1)%len(ck.servers) == 0 {
			time.Sleep(raft.HEARTBEAT * 3)
		}

	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	arg := &PutAppendArgs{
		ClerkID: ck.ClerkID,
		OpSeq:   ck.OpSeq,
		Key:     key,
		Value:   value,
		Kind:    op,
	}
	ck.OpSeq++
	time.Sleep(1 * time.Millisecond)

	co := 0
	for {
		Debug(DClient, "[*] C%d SEND PA_REQ TO S%d", ck.ClerkID, ck.leaderID)
		reply := &PutAppendReply{}

		retCh := make(chan bool, 1) // 这里必须是带缓冲的，为了能够让工作协程顺利退出
		go func() {
			retCh <- ck.servers[ck.leaderID].Call("KVServer.PutAppend", arg, reply)
		}()

		var ok bool
		select {
		case ok = <-retCh:
		case <-time.After(raft.HEARTBEAT * 10):
			ok = false
		}

		// Call返回false或者定时器到期，表明请求超时
		if !ok {
			Debug(DClient, "[*] C%d RPC:%d TIMEOUT", ck.ClerkID, ck.OpSeq-1)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			co++
		} else {
			Debug(DClient, "[*] C%d PA REPLY:%+v", ck.ClerkID, reply)
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
				ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
				co++
			}

		}
		// 如果所有的server都不是leader，那就等待300ms
		if co%len(ck.servers) == 0 {
			time.Sleep(raft.HEARTBEAT * 3)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
