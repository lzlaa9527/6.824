package shardctrler

import (
	. "6.824/common"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// Your data here.
	dead int32

	configs  []Config // indexed by config num
	OpReplys          // 存储server已经处理的Op及其结果
	ITable            // 记录每个客户端待处理的Op二元组标识符：(ClerkID, OpSeq)；需要持久化保存
	configer Configer
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		ServerID: sc.me,
		Kind:     "Join",
		Key:      args.Servers,

		ID: Identifier{
			ClerkID: args.ClerkID,
			Seq:     args.OpSeq,
		},
	}
	Debug(DServer, "[*] S%d RECEIVE OP:%+v", sc.me, op)

	if ok, ret := sc.ITable.Executed(op.ID); ok {
		reply.Err = ret.(JoinReply).Err
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(DServer, "[*] S%d SEND RAFT, WAIT: %d.", sc.me, index)

	ret, err := sc.WaitAndMatch(index, op)
	if ret == nil {
		reply.Err = err
	} else {
		reply.Err = ret.(JoinReply).Err
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		ServerID: sc.me,
		Kind:     "Leave",
		Key:      args.GIDs,

		ID: Identifier{
			ClerkID: args.ClerkID,
			Seq:     args.OpSeq,
		},
	}
	Debug(DServer, "[*] S%d RECEIVE OP:%+v", sc.me, op)

	if ok, ret := sc.ITable.Executed(op.ID); ok {
		reply.Err = ret.(LeaveReply).Err
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(DServer, "[*] S%d SEND RAFT, WAIT: %d.", sc.me, index)

	ret, err := sc.WaitAndMatch(index, op)
	if ret == nil {
		reply.Err = err
	} else {
		reply.Err = ret.(LeaveReply).Err
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {

	// Your code here.
	op := Op{
		ServerID: sc.me,
		Kind:     "Move",
		Key:      args.Shard,
		Value:    args.GID,
		ID: Identifier{
			ClerkID: args.ClerkID,
			Seq:     args.OpSeq,
		},
	}
	Debug(DServer, "[*] S%d RECEIVE OP:%+v", sc.me, op)

	if ok, ret := sc.ITable.Executed(op.ID); ok {
		reply.Err = ret.(MoveReply).Err
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(DServer, "[*] S%d SEND RAFT, WAIT: %d.", sc.me, index)

	ret, err := sc.WaitAndMatch(index, op)
	if ret == nil {
		reply.Err = err
	} else {
		reply.Err = ret.(MoveReply).Err
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	num := args.Num
	if num > 0 && num < len(sc.configs) { // 直接返回
		reply.Config = sc.configs[num]
		reply.Err = OK
		return
	}

	op := Op{
		ServerID: sc.me,
		Kind:     "Query",
		Key:      args.Num,

		ID: Identifier{
			ClerkID: args.ClerkID,
			Seq:     args.OpSeq,
		},
	}
	Debug(DServer, "[*] S%d RECEIVE OP:%+v", sc.me, op)

	if ok, ret := sc.ITable.Executed(op.ID); ok {
		reply.Err = ret.(QueryReply).Err
		reply.Config = ret.(QueryReply).Config
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		Debug(DServer, "[*] S%d Not LEADER!!!", sc.me)
		reply.Err = ErrWrongLeader
		return
	}
	Debug(DServer, "[*] S%d SEND RAFT, WAIT: %d.", sc.me, index)

	ret, err := sc.WaitAndMatch(index, op)
	if ret == nil {
		reply.Err = err
	} else {
		reply.Err = ret.(QueryReply).Err
		reply.Config = ret.(QueryReply).Config
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
	Debug(DServer, "S%d Stop!", sc.me)

}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryReply{})
	labgob.Register(map[int][]string{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.ITable = NewITable()
	sc.OpReplys = NewOpReplays()
	sc.configer = newDefaultConfiger()
	go sc.applier()

	Debug(DServer, "[*] S%d start.", me)
	return sc
}

// 所有的请求都需要交由底层的raft实现备份提交之后，才会被真正的执行
func (sc *ShardCtrler) applier() {
	for applyMsg := range sc.applyCh {
		if sc.killed() {
			return
		}

		// ShardCtrler 不用处理快照
		if !applyMsg.CommandValid {
			continue
		}

		Debug(DServer, "[*] S%d RECEIVE LOG ENTRY. IN:%d, CMD:%+v", sc.me, applyMsg.CommandIndex, applyMsg.Command)

		op := applyMsg.Command.(Op)
		identifier := op.ID
		index := applyMsg.CommandIndex

		// 避免重复执行同一个op
		if ok, reply := sc.ITable.Executed(op.ID); ok {
			sc.OpReplys.SetAndBroadcast(Index(index), op, reply)
			continue
		}

		var reply interface{}

		// 执行对应的命令
		switch op.Kind {
		case "Join":
			sc.configer.Join(op.Key.(map[int][]string))
			sc.configs = append(sc.configs, sc.configer.Export(len(sc.configs)))
			reply = JoinReply{Err: OK}
		case "Leave":
			sc.configer.Leave(op.Key.([]int))
			sc.configs = append(sc.configs, sc.configer.Export(len(sc.configs)))
			reply = LeaveReply{Err: OK}
		case "Move":
			sc.configer.Move(op.Key.(int), op.Value.(int))
			sc.configs = append(sc.configs, sc.configer.Export(len(sc.configs)))
			reply = MoveReply{OK}
		case "Query":
			num := op.Key.(int)
			ret := QueryReply{Err: OK}
			if num == -1 || num >= len(sc.configs) {
				ret.Config = sc.configs[len(sc.configs)-1]
			} else {
				ret.Config = sc.configs[num]
			}
			reply = ret
		}

		// 更新clerkID对应的Client的下一个待执行Op的Seq
		sc.ITable.UpdateIdentifier(identifier.ClerkID, identifier.Seq+1, reply)

		sc.OpReplys.SetAndBroadcast(Index(index), op, reply)
	}
}
