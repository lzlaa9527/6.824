package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int

	// Your definitions here.
	ITable        // 记录每个客户端已经处理了的Op序列号，二元组：(ClerkID, OpSeq)是客户端Op的唯一标识符
	OpResults     // 存储server已经处理的Op及其结果
	Database      // 数据库
	OpIndex   int // 已经处理的op序列号，初始化为1
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	clerkID := args.ClerkID
	op := Op{
		ServerID: kv.me,
		Kind:     "Get",
		Key:      args.Key,

		// GetIdentifier(clerkID)获取clerkID对应的客户端下一个Op对应的Seq
		ID: kv.GetIdentifier(clerkID),
	}
	Debug(dServer, "[*] S%d RECEIVE OP:%+v", kv.me, op)

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader

		Debug(dServer, "[*] S%d NOT LEADER.", kv.me)
		return
	}
	Debug(dServer, "[*] S%d SEND RAFT, WAIT: %d.", kv.me, index)

	ret, err := kv.waitAndMatch(index, op)
	Debug(dServer, "[*] S%d GET REPLY, SEQ: %d.", kv.me, index)
	if ret != nil {
		reply.Err = ret.(*GetReply).Err
		reply.Value = ret.(*GetReply).Value
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	clerkID := args.ClerkID

	op := Op{
		ServerID: kv.me,
		Kind:     args.Kind,
		Key:      args.Key,
		Value:    args.Value,
		// GetIdentifier(clerkID)获取clerkID对应的客户端下一个Op对应的Seq
		ID: kv.GetIdentifier(clerkID),
	}

	Debug(dServer, "[*] S%d RECEIVE OP:%+v", kv.me, op)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		Debug(dServer, "[*] S%d NOT LEADER.", kv.me)
		return
	}
	Debug(dServer, "[*] S%d SEND RAFT, WAIT: %d.", kv.me, index)

	ret, err := kv.waitAndMatch(index, op)
	Debug(dServer, "[*] S%d GET REPLY, SEQ: %d.", kv.me, index)
	if ret != nil {
		reply.Err = ret.(*PutAppendReply).Err
	} else {
		reply.Err = err
	}
}

// clerk协程等待reqOp的完成。
// 因为存在raft leadership的转变，多个clerk可能会等待同一个index的op；当server执行完
// index对应的op后，会通知所有等待的clerk协程；clerk协程会根据result.op == reqOp判断
// 完成的op是不是自己提交的op；如果不是就表明发生了leadership的转变
//
func (kv *KVServer) waitAndMatch(index int, reqOp Op) (interface{}, Err) {

	result := kv.OpResults.Wait(Index(index))
	resOp := result.op // raft所提交日志中的ApplyMsg

	// leadership 发生变更，或者原先提交的请求被覆盖时index处的 resOp != reqOp
	if resOp != reqOp {
		return nil, ErrWrongLeader
	}
	kv.OpResults.Delete(Index(index)) // 为了节约内存及时删除缓存
	return result.reply, ""
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.OpResults.Destory() // 通知尚未退出的clerk退出
	close(kv.applyCh)      // 通知applier协程退出
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/msg service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.ITable = make(ITable)
	kv.OpResults = OpResults{
		table: make(map[Index]*OpResult),
		mu:    new(sync.Mutex),
	}
	kv.Database = make(Database)

	go kv.applier()

	Debug(dServer, "[*] S%d start.", me)
	return kv
}

// applier是唯一改变的server状态的工作协程，主要执行如下工作：
// 1. applier安装raft提交的快照，重建Database，以及ITable。
// 2. applier协程用来执行被raft提交的op更新数据库状态，并且唤醒等待该op的clerk协程；
// 3. 如果raft的statesize达到maxstatesize，就通知raft拍摄快照
func (kv *KVServer) applier() {
	for applyMsg := range kv.applyCh {
		if kv.killed() {
			return
		}

		Debug(dServer, "[*] S%d RECEIVE MSG:%+v", kv.me, applyMsg)

		if applyMsg.SnapshotValid { // 应用snapshot
			kv.InstallSnapshot(applyMsg.Snapshot)
		} else { // 应用普通的日志条目
			if !applyMsg.CommandValid {
				continue
			}
			op := applyMsg.Command.(Op)
			identifier := op.ID
			index := applyMsg.CommandIndex

			// 忽略唯一标识符重复的op
			// 避免因为leadership变更，导致一个op重复执行
			if kv.ITable.Executed(identifier) {
				continue
			}

			// 更新clerkID对应的Client的下一个Op的Seq
			kv.ITable.UpdateIdentifier(identifier.ClerkID)

			var reply interface{}
			// 执行对应的命令
			switch op.Kind {
			case "Get":
				reply = kv.Database.Get(op.Key)
			case "Put":
				reply = kv.Database.Put(op.Key, op.Value)
			case "Append":
				reply = kv.Database.Append(op.Key, op.Value)
			}

			// 唤醒等待op执行结果的clerk协程。
			// 如果op.ServerID == kv.me说明该op是通过当前Server提交的，并且
			// 当applyMsg.DupCommitted == false时说明该op是在server重启后提交的。
			//
			// 重启前提交的op需要被重放，但是不存在clerk协程等待server重启前提交的op。
			kv.OpResults.SetAndBroadcast(Index(index), op, reply, op.ServerID == kv.me && !applyMsg.DupCommitted)
			Debug(dServer, "[*] S%d HANDLE OP:%+v, REPLY:%+v", kv.me, op, reply)
		}

		// 通知raft进行snapshot
		if kv.maxraftstate <= kv.rf.RaftStateSize() {
			var snap []byte
			kv.rf.Snapshot(applyMsg.CommandIndex, snap)
		}
	}
}

// 拍摄快照
func (kv *KVServer) Snapshot() []byte {
	snapshot := new(bytes.Buffer)
	e := labgob.NewEncoder(snapshot)
	if err := e.Encode(&kv.Database); err != nil {
		log.Fatalf("S%d fail to encode database, err:%v\n", kv.me, err)
	}

	if err := e.Encode(&kv.ITable); err != nil {
		log.Fatalf("S%d fail to encode ITable, err:%v\n", kv.me, err)
	}

	return snapshot.Bytes()
}

// 安装快照
func (kv *KVServer) InstallSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&kv.Database); err != nil {
		log.Fatalf("S%d fail to decode database, err:%v\n", kv.me, err)
	}

	if err := d.Decode(&kv.ITable); err != nil {
		log.Fatalf("S%d fail to decode ITable, err:%v\n", kv.me, err)
	}
}
