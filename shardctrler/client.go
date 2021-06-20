package shardctrler

//
// Shardctrler clerk.
//

import (
	. "6.824/common"
	"6.824/labrpc"
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

	// 避免因为执行太快，导致测试代码误判不满足线性一致性。
	time.Sleep(time.Millisecond * 1)

	t := time.NewTimer(time.Second)
	replyType := reflect.TypeOf(reply).Elem()
	for {

		reply = reflect.New(replyType).Interface()
		retCh := make(chan bool, 1) // 这里必须是带缓冲的，为了能够让工作协程顺利退出
		go func() {
			Debug(DClient, "[*] C%d CALL %s TO S%d, SEQ:%d", ck.ClerkID, method, ck.leaderID, ck.OpSeq-1)
			retCh <- ck.servers[ck.leaderID].Call(method, arg, reply)
		}()

		ResetTimer(t, time.Second)
		select {
		case <-retCh:
			t.Stop()
		case <-t.C:
			Debug(DClient, "[*] C%d CALL %s TO S%d TIMEOUT. SEQ:%d", ck.ClerkID, method, ck.leaderID, ck.OpSeq-1)
		}

		Debug(DClient, "[*] C%d RECEIVE %s REPLY, SEQ: %d; %+v", ck.ClerkID, method, ck.OpSeq-1, reply)
		switch reflect.ValueOf(reply).Elem().FieldByName("Err").Interface().(Err) {
		case OK:
			return reply
		case ErrWrongLeader:
			Debug(DClient, "[*] S%d WRONG LEADER.", ck.leaderID)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}

		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)

		// 所有的server都不是leader；
		// 就等待一会等他们选举出leader。
		if ck.leaderID%len(ck.servers) == 0 {
			time.Sleep(time.Millisecond * 100)
		}
	}
}
