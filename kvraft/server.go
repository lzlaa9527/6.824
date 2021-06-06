package kvraft

import (
	. "6.824/common"
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
	OpReplys // 存储server已经处理的Op及其结果
	ITable   // 记录每个客户端待处理的Op二元组标识符：(ClerkID, OpSeq)；需要持久化保存
	Database // 数据库；需要持久化保存
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op{
		ServerID: kv.me,
		Kind:     "Get",
		Key:      args.Key,

		ID: Identifier{
			ClerkID: args.ClerkID,
			Seq:     args.OpSeq,
		},
	}
	Debug(DServer, "[*] S%d RECEIVE OP:%+v", kv.me, op)

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(DServer, "[*] S%d SEND RAFT, WAIT: %d.", kv.me, index)

	ret, err := kv.WaitAndMatch(index, op)
	if ret == nil {
		reply.Err = err
	} else {
		reply.Err = ret.(GetReply).Err
		reply.Value = ret.(GetReply).Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := Op{
		ServerID: kv.me,
		Kind:     args.Kind,
		Key:      args.Key,
		Value:    args.Value,
		ID: Identifier{
			ClerkID: args.ClerkID,
			Seq:     args.OpSeq,
		},
	}

	Debug(DServer, "[*] S%d RECEIVE OP:%+v", kv.me, op)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ret, err := kv.WaitAndMatch(index, op)
	if ret == nil {
		reply.Err = err
	} else {
		reply.Err = ret.(PutAppendReply).Err
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.OpReplys.Destory() // 通知尚未退出的clerk退出
	Debug(DServer, "S%d Stop!", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// 将可能作为interface{}取值的字段，进行注册
	labgob.Register(Op{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.ITable = NewITable()
	kv.OpReplys = NewOpReplays()
	kv.Database = make(Database)

	go kv.applier()

	Debug(DServer, "[*] S%d start.", me)
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

		if applyMsg.SnapshotValid { // 应用snapshot
			Debug(DServer, "[*] S%d INSTALL SNAPSHOT. IN:%d, TERM:%d", kv.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)

			kv.InstallSnapshot(applyMsg.Snapshot)
		} else { // 应用普通的日志条目
			Debug(DServer, "[*] S%d RECEIVE LOG ENTRY. IN:%d, CMD:%+v", kv.me, applyMsg.CommandIndex, applyMsg.Command)

			op := applyMsg.Command.(Op)
			identifier := op.ID
			index := applyMsg.CommandIndex

			// 避免重复执行同一个op
			if kv.ITable.Executed(identifier) {
				reply := kv.ITable.GetCacheReply(op.ID.ClerkID)
				kv.OpReplys.SetAndBroadcast(Index(index), op, reply, op.ServerID == kv.me && !applyMsg.Replay)
				continue
			}

			var reply interface{}
			// 执行对应的命令
			switch op.Kind {
			case "Get":
				reply = kv.Database.Get(op.Key.(string))
			case "Put":
				reply = kv.Database.Put(op.Key.(string), op.Value.(string))
			case "Append":
				reply = kv.Database.Append(op.Key.(string), op.Value.(string))
			}

			// 更新clerkID对应的Client的下一个待执行Op的Seq
			kv.ITable.UpdateIdentifier(identifier.ClerkID, identifier.Seq+1, reply)

			// 唤醒等待op执行结果的clerk协程。
			// 如果op.ServerID == kv.me说明该op是通过当前Server提交的，并且
			// 当applyMsg.Replay == false时说明该op是在server重启后提交的。
			//
			// 重启前提交的op需要被重放，但是不存在clerk协程等待server重启前提交的op。
			kv.OpReplys.SetAndBroadcast(Index(index), op, reply, op.ServerID == kv.me && !applyMsg.Replay)
		}

		// 通知raft进行snapshot
		// 一定要在安装完快照之后才拍摄新的快照
		// 否则若在安装快照之前拍摄新的快照：当raft宕机恢复之后判断raft.statesize足够大了，此时数据库的状态为空！
		if kv.maxraftstate > -1 && kv.maxraftstate <= kv.rf.RaftStateSize() {
			Debug(DServer, "[*] S%d SNAPSHOT.", kv.me)
			kv.rf.Snapshot(applyMsg.CommandIndex, kv.Snapshot())
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

	if err := e.Encode(kv.ITable); err != nil {
		log.Fatalf("S%d fail to encode ITable, err:%v\n", kv.me, err)
	}

	// fmt.Printf("[*] C%d, snapshot:\ndatabase:%v\nitable:%v\n", kv.me, kv.Database, kv.ITable.SeqTable)
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

	// fmt.Printf("[*] C%d, install snapshot:\ndatabase:%v\nitable:%v\n", kv.me, kv.Database, kv.ITable.SeqTable)
}
