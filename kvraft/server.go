package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
)

type signal struct{}

type Result struct {
	s     chan signal
	op    Op
	reply interface{}
}

type Index int

// 存储 raft Index处对应的Op，及其执行结果
type Results map[Index]*Result

// 阻塞在i处的Op对应的signal channel上，并在被唤醒后获取到Op以及对应的Result
// 当Op被执行之后applier协程通过关闭该signal channel通知阻塞的工作协程
// 如果调用协程是第一个调用者，首先初始化该signal
func (r Results) Wait(i Index) *Result {
	ret, ok := r[i]
	if ok == false {
		r[i] = &Result{
			s: make(chan signal),
		}
		ret = r[i]
		<-ret.s
	} else {
		<-ret.s
	}
	return ret
}

func (r Results) SetAndBroadcast(i Index, op Op, re interface{}) {
	ret, ok := r[i]
	if !ok { // 没有等待该Op的工作协程，直接返回
		return
	}
	ret.op = op
	// 设置op的执行结果
	ret.reply = re
	// 唤醒等待协程
	close(ret.s)
}

func (r Results) Delete(i Index) {
	delete(r, i)
}

// 每一个Client的Op都有一个唯一标识符Identifier
// ClerkID	标识Client ID
// Seq		标识Client下一个Op的序号
type Identifier struct {
	ClerkID int
	Seq     int
}

// ITable是Server记录目前各个Client待提交Op的Seq的哈希表
// 关键字是Client的ClerkID
// 值是该Client目前待提交的Seq
type ITable map[int]int

// 返回clerkID对应Client的可用的Op标识符
func (itable ITable) GetIdentifier(clerkID int) Identifier {

	return Identifier{
		ClerkID: clerkID,
		Seq:     itable[clerkID],
	}
}

// 更新clerkID对应Client的下一个Op标识符
func (itable ITable) UpdateIdentifier(clerkID int) {
	itable[clerkID]++
}

// 如果i标识的Op已经被执行过了，Executed返回true
func (itable ITable) Executed(i Identifier) bool {
	return i.Seq < itable[i.ClerkID]
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind  string
	Key   string
	Value string
	ID    Identifier
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ITable // 用来记录每个客户端已经处理了的Op序列号
	Results
	Database // 数据库
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	clerkID := args.ClerkID
	op := Op{
		Kind: "Get",
		Key:  args.Key,

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

	Debug(dServer, "[*] S%d SEND MSG TO RAFT.", kv.me)

	ret, err := kv.waitAndMatch(index, op)
	if ret != nil {
		reply.Err = ret.(*GetReply).Err
		reply.Value = ret.(*GetReply).Value
	} else {
		reply.Err = err
	}
}

// 工作协程阻塞在条件变量kv.WithValueCond等待raft提交日志，
// 被唤醒后并判断raft提交的日志是不是记录op所标识的任务。
// 日志记录在kv.WithValueCond中
//
func (kv *KVServer) waitAndMatch(index int, reqOp Op) (interface{}, Err) {

	result := kv.Results.Wait(Index(index))
	resOp := result.op // raft所提交日志中的ApplyMsg

	// leadership 发生变更，或者原先提交的请求被覆盖时index处的 resOp != reqOp
	if resOp != reqOp {
		return nil, ErrWrongLeader
	}
	kv.Results.Delete(Index(index)) // 为了节约内存及时删除缓存
	return result.reply, ""
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	clerkID := args.ClerkID

	op := Op{
		Kind:  args.Kind,
		Key:   args.Key,
		Value: args.Value,
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

	Debug(dServer, "[*] S%d SEND MSG TO RAFT.", kv.me)
	ret, err := kv.waitAndMatch(index, op)
	if ret != nil {
		reply.Err = ret.(*PutAppendReply).Err
	} else {
		reply.Err = err
	}
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
	// Your code here, if desired.
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
	kv.Results = make(Results)
	kv.Database = make(Database)

	go kv.applier()

	Debug(dServer, "[*] S%d start.", me)
	return kv
}

// 监听kv.applyCh并检测每一个被提交的日志中所包含的命令(Kind)，
// 通过检测命令中的唯一标识符判断该命令是否被提交两次。
// 如果是，不被执行但直接返回结果
// 如果不是，执行并返回结果，同时更新对应Client下一个Op的标识符
func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		Debug(dServer, "[*] S%d RECEIVE MSG:%+v", kv.me, msg)
		if !msg.CommandValid {
			continue
		}
		op := msg.Command.(Op)
		identifier := op.ID
		index := msg.CommandIndex

		// 忽略被重复提交的命令
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

		Debug(dServer, "[*] S%d HANDLE OP:%+v, REPLY:%+v", kv.me, op, reply)

		// 唤醒等待op执行结果的工作协程
		kv.Results.SetAndBroadcast(Index(index), op, reply)
	}
}
