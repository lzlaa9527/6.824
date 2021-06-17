package shardkv

//
// client code to talk to a sharded key/DB service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	. "6.824/common"
	"6.824/labrpc"
	"6.824/raft"
	"reflect"
)
import "6.824/shardctrler"
import "time"

var ClerkID int

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	leaderID int // 目前所知的leader ID
	ClerkID  int
	OpSeq    int // clerk下一个Op使用的sequence number
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.leaderID = 0
	ck.OpSeq = 0
	ck.config = shardctrler.Config{
	}

	ck.ClerkID = ClerkID
	ClerkID++

	return ck
}

//
// fetch the current DB for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key:     key,
		Cfgnum:  ck.config.Num,
		ClerkID: ck.ClerkID,
		OpSeq:   ck.OpSeq,
	}
	ck.OpSeq++

	return ck.doRPC("ShardKV.Get", key, args, &GetReply{}).(*GetReply).Value
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:     key,
		Value:   value,
		Kind:    op,
		Cfgnum:  ck.config.Num,
		ClerkID: ck.ClerkID,
		OpSeq:   ck.OpSeq,
	}
	ck.OpSeq++

	ck.doRPC("ShardKV.PutAppend", key, args, &PutAppendReply{})
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 如果servers != nil 表明调用ShardKV.Pull：从当前的数据库中拉取shardID对于的shard
// 否则就是调用ShardKV.Put/Append/Get
func (ck *Clerk) doRPC(method string, key string, arg interface{}, reply interface{}) interface{} {

	co := 0
	replyType := reflect.TypeOf(reply).Elem()
	for {

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, existed := ck.config.Groups[gid]

		if !existed {
			ck.config = ck.sm.Query(-1)
			reflect.ValueOf(arg).Elem().FieldByName("Cfgnum").SetInt(int64(ck.config.Num))
			Debug(DClient, "[*] C%d FETCH CONFIG: %d#%+v", ck.ClerkID, ck.config.Num, ck.config.Shards)
			continue
		}

		co++
		Debug(DClient, "[*] C%d CALL `%s` TO S%d#%d, SEQ:%d, CFG:%d", ck.ClerkID, method, ck.leaderID, gid, ck.OpSeq-1, ck.config.Num)
		reply = reflect.New(replyType).Interface()
		retCh := make(chan bool, 1) // 这里必须是带缓冲的，为了能够让工作协程顺利退出
		go func() {
			srv := ck.make_end(servers[ck.leaderID])
			retCh <- srv.Call(method, arg, reply)
		}()

		var ok bool
		select {
		case ok = <-retCh:
		case <-time.After(raft.HEARTBEAT * 10):
			ok = false
		}

		// Call返回false或者定时器到期，表明请求超时
		if !ok {
			Debug(DClient, "[*] C%d CALL `%s` TIMEOUT, SEQ: %d", ck.ClerkID, method, ck.OpSeq-1)

			ck.config = ck.sm.Query(-1)
			reflect.ValueOf(arg).Elem().FieldByName("Cfgnum").SetInt(int64(ck.config.Num))
			Debug(DClient, "[*] C%d FETCH CONFIG: %d#%+v", ck.ClerkID, ck.config.Num, ck.config.Shards)

			co = 0
			ck.leaderID = (ck.leaderID + 1) % len(servers)
		} else {
			Debug(DClient, "[*] C%d RECEIVE `%s` REPLY, SEQ: %d; %+v", ck.ClerkID, method, ck.OpSeq-1, reply)

			switch reflect.ValueOf(reply).Elem().FieldByName("Err").Interface().(Err) {
			case OK, ErrNoKey:
				return reply

			case ErrWrongLeader:
				Debug(DClient, "[*] S%d WRONG LEADER.", ck.leaderID)
				ck.leaderID = (ck.leaderID + 1) % len(servers)

			case ErrWrongConfig: // clerk 需要更新config
				time.Sleep(raft.HEARTBEAT)
				ck.config = ck.sm.Query(-1)
				reflect.ValueOf(arg).Elem().FieldByName("Cfgnum").SetInt(int64(ck.config.Num))
				Debug(DClient, "[*] C%d FETCH CONFIG: %d#%+v", ck.ClerkID, ck.config.Num, ck.config.Shards)
				continue
			}
		}

		// 如果所有的server都不是leader，那就等待300ms
		if co%len(servers) == 0 {
			time.Sleep(raft.HEARTBEAT * 3)
		}
	}
}
