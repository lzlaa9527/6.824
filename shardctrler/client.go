package shardctrler

//
// Shardctrler clerk.
//

import (
	. "6.824/common"
	"6.824/labrpc"
	"6.824/raft"
	"reflect"
)
import "time"

var ClerkID int

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	leaderID int // 目前所知的leader ID
	ClerkID  int
	OpSeq    int // clerk下一个Op使用的sequence number
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.

	ck.leaderID = 0

	ck.ClerkID = ClerkID
	ClerkID++

	Debug(DClient, "[*] Shardctrler clerk%d init.", ck.ClerkID)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:     num,
		ClerkID: ck.ClerkID,
		OpSeq:   ck.OpSeq,
	}
	// Your code here.
	ck.OpSeq++

	ret := ck.doRPC("ShardCtrler.Query", args, &QueryReply{}).(*QueryReply)
	return ret.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers: servers,
		ClerkID: ck.ClerkID,
		OpSeq:   ck.OpSeq,
	}
	ck.OpSeq++

	ck.doRPC("ShardCtrler.Join", args, &JoinReply{})
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:    gids,
		ClerkID: ck.ClerkID,
		OpSeq:   ck.OpSeq,
	}
	// Your code here.
	ck.OpSeq++

	ck.doRPC("ShardCtrler.Leave", args, &LeaveReply{})
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:   shard,
		GID:     gid,
		ClerkID: ck.ClerkID,
		OpSeq:   ck.OpSeq,
	}
	// Your code here.
	ck.OpSeq++

	ck.doRPC("ShardCtrler.Move", args, &MoveReply{})
}

func (ck *Clerk) doRPC(method string, arg interface{}, reply interface{}) interface{} {

	co := 0

	replyType := reflect.TypeOf(reply).Elem()
	for {
		Debug(DClient, "[*] C%d CALL %s TO S%d, SEQ:%d", ck.ClerkID, method, ck.leaderID, ck.OpSeq-1)

		reply = reflect.New(replyType).Interface()
		retCh := make(chan bool, 1) // 这里必须是带缓冲的，为了能够让工作协程顺利退出
		go func() {
			retCh <- ck.servers[ck.leaderID].Call(method, arg, reply)
		}()

		var ok bool
		select {
		case ok = <-retCh:
		case <-time.After(raft.HEARTBEAT * 10):
			ok = false
		}

		// Call返回false或者定时器到期，表明请求超时
		if !ok {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			co++
			Debug(DClient, "[*] C%d CALL %s TIMEOUT, SEQ: %d", ck.ClerkID, method, ck.OpSeq-1)
		} else {
			Debug(DClient, "[*] C%d RECEIVE %s REPLY, SEQ: %d; %+v", ck.ClerkID, method, ck.OpSeq-1, reply)

			switch reflect.ValueOf(reply).Elem().FieldByName("Err").Interface().(Err) {
			case OK:
				return reply
			case ErrWrongLeader:
				Debug(DClient, "[*] S%d WRONG LEADER.", ck.leaderID)
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
