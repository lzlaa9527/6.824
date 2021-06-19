package shardkv

import (
	. "6.824/common"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()
	// Your definitions here.

	mck    *shardctrler.Clerk // kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	config shardctrler.Config // 当前应用的config

	OpReplys // 存储server已经处理的Op及其结果
	ITable   // 记录每个客户端待处理的Op二元组标识符：(ClerkID, OpSeq)；需要持久化保存
	Database // 数据库；需要持久化保存

	pullTasks Tasks
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	op := Op{
		ServerID: kv.me,
		Kind:     "Get",
		Key:      args.Key,
		Cfgnum:   args.Cfgnum,
		ID: Identifier{
			ClerkID: args.ClerkID,
			Seq:     args.OpSeq,
		},
	}

	if ok, ret := kv.ITable.Executed(op.ID); ok {
		reply.Err = ret.(GetReply).Err
		reply.Value = ret.(GetReply).Value
		return
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
	}

	// 当前数据尚未迁移完成
	if kv.pullTasks.Contains(key2shard(args.Key)) {
		reply.Err = ErrMigrating
		return
	}

	curCfgnum := atomic.LoadInt64((*int64)(unsafe.Pointer(&kv.config.Num)))
	Debug(DServer, "[*] S%d#%d&%d RECEIVE `Get` OP:%+v", kv.me, kv.gid, curCfgnum, args)

	if args.Cfgnum < int(curCfgnum) {
		reply.Err = ErrLowerConfig
		return
	} else if args.Cfgnum > int(curCfgnum) {
		reply.Err = ErrHigherConfig
		return
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ret, err := kv.WaitAndMatch(index, op)
	if ret == nil {
		reply.Err = err
	} else {
		reply.Err = ret.(GetReply).Err
		reply.Value = ret.(GetReply).Value
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := Op{
		ServerID: kv.me,
		Kind:     args.Kind,
		Key:      args.Key,
		Value:    args.Value,
		Cfgnum:   args.Cfgnum,
		ID: Identifier{
			ClerkID: args.ClerkID,
			Seq:     args.OpSeq,
		},
	}

	if ok, ret := kv.ITable.Executed(op.ID); ok {
		reply.Err = ret.(PutAppendReply).Err
		return
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
	}

	// 当前数据尚未迁移完成
	if kv.pullTasks.Contains(key2shard(args.Key)) {
		reply.Err = ErrMigrating
		return
	}

	curCfgnum := atomic.LoadInt64((*int64)(unsafe.Pointer(&kv.config.Num)))
	Debug(DServer, "[*] S%d#%d&%d RECEIVE `PutAppend` OP:%+v", kv.me, kv.gid, curCfgnum, args)

	if args.Cfgnum < int(curCfgnum) {
		reply.Err = ErrLowerConfig
		return
	} else if args.Cfgnum > int(curCfgnum) {
		reply.Err = ErrHigherConfig
		return
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(DServer, "[*] S%d#%d&%d SEND RAFT, WAIT: %d.", kv.me, kv.gid, curCfgnum, index)

	ret, err := kv.WaitAndMatch(index, op)
	if ret == nil {
		reply.Err = err
	} else {
		reply.Err = ret.(PutAppendReply).Err
	}
}

// Pull 向其它group提供的拉取数据的接口。
// 要求args.Cfgnum < config.Num，也就是说当reconfig与数据库操作请求同时到达时
// 必须先完成请求数据库操作请求，再进行数据迁移。
func (kv *ShardKV) Pull(args *PullArgs, reply *PullReply) {

	op := Op{
		ServerID: kv.me,
		Kind:     "Pull",
		Value:    *args,
		Cfgnum:   args.Cfgnum,

		// 为了避免了Pull请求的重复执行！！
		// 需要为其设置唯一标识符。
		// Identifier中ClerkID的最高位为1，标识该请求来自于server而不是Clerk。
		ID: Identifier{
			ClerkID: (1 << 62) | (args.FromGID << 31) | args.ToGID,
			Seq:     args.Cfgnum,
		},
	}

	// 如果只检查Executed，存在数据冲突的情况。
	// 在执行Exectued后执行InstallSnapshot，Executed检查通过，但是GetCacheReply返回nil
	if ok, ret := kv.ITable.Executed(op.ID); ok {
		Debug(DServer, "[*] S%d#%d&%d EXECUTED `Pull` OP:%+v", kv.me, kv.gid, kv.config.Num, args)

		reply.Err = ret.(PullReply).Err
		reply.DB = ret.(PullReply).DB

		reply.SeqTable, reply.ReplyTable = kv.ITable.Export(false)
		return
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
	}

	curCfgnum := atomic.LoadInt64((*int64)(unsafe.Pointer(&kv.config.Num)))
	Debug(DServer, "[*] S%d#%d&%d RECEIVE `Pull` OP:%+v", kv.me, kv.gid, curCfgnum, args)

	if args.Cfgnum > int(curCfgnum) {
		reply.Err = ErrHigherConfig
		return
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ret, err := kv.WaitAndMatch(index, op)
	if ret == nil {
		reply.Err = err
	} else {
		reply.Err = ret.(PullReply).Err
		reply.DB = ret.(PullReply).DB
		reply.SeqTable, reply.ReplyTable = kv.ITable.Export(false)
	}
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.OpReplys.Destory() // 通知尚未退出的clerk退出
	Debug(DServer, "S%d#%d Stop!", kv.me, kv.gid)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
	gid int,
	ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {

	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetReply{})
	labgob.Register(PullReply{})
	labgob.Register(PullArgs{})
	labgob.Register(MergeReply{})
	labgob.Register(MergeArgs{})
	labgob.Register(ReConfigReply{})
	labgob.Register(ReConfigArgs{})
	labgob.Register(Database{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.SetGID(kv.gid)

	kv.ITable = NewITable()
	kv.OpReplys = NewOpReplays()
	kv.Database = make(Database)
	kv.pullTasks = NewTasks()

	go kv.applier()
	go kv.poll()

	Debug(DServer, "[*] S%d#%d start. ", me, kv.gid)
	return kv
}

// applier是唯一改变的server状态的工作协程，主要执行如下工作：
// 1. applier安装raft提交的快照，重建Database，以及ITable。
// 2. applier协程用来执行被raft提交的op更新数据库状态，并且唤醒等待该op的clerk协程；
// 3. 如果raft的statesize达到maxstatesize，就通知raft拍摄快照
func (kv *ShardKV) applier() {
	for applyMsg := range kv.applyCh {

		if kv.killed() {
			Debug(DServer, "[*] S%d#%d&%d applier crash. IN:%d, TERM:%d", kv.me, kv.gid, kv.config.Num)
			return
		}

		if applyMsg.SnapshotValid { // 应用snapshot
			Debug(DServer, "[*] S%d#%d&%d INSTALL SNAPSHOT. IN:%d, TERM:%d", kv.me, kv.gid, kv.config.Num, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			kv.InstallSnapshot(applyMsg.Snapshot)

		} else { // 应用普通的日志条目
			op := applyMsg.Command.(Op)
			identifier := op.ID
			index := applyMsg.CommandIndex
			Debug(DServer, "[*] S%d#%d&%d RECEIVE LOG ENTRY. IN:%d, CMD:%+v", kv.me, kv.gid, kv.config.Num, applyMsg.CommandIndex, applyMsg.Command)

			// 避免重复执行同一个op
			if ok, reply := kv.ITable.Executed(op.ID); ok {
				kv.OpReplys.SetAndBroadcast(Index(index), op, reply, op.ServerID == kv.me && !applyMsg.Replay)
				continue
			}

			// 如果一个请求因为WrongConfig而失败，则它可以被重新执行；
			// 此时retry为true。
			retry := false
			var reply interface{}
			switch op.Kind {
			case "Pull":
				if op.Cfgnum > kv.config.Num {
					reply = PullReply{Err: ErrHigherConfig}
					retry = true
				} else {
					pullArgs := op.Value.(PullArgs)
					pullDB := make(Database)
					for k, v := range kv.Database {
						shardID := key2shard(k)
						for _, sid := range pullArgs.Shards {
							if sid == shardID {
								pullDB[k] = v
								delete(kv.Database, k)
							}
						}
					}

					// 数据迁移时还需要同步不同备份组的ITable；
					// 避免同一个请求在不同的备份组，被重复执行。
					// 这里只同步来自Clerk的请求，不同步来自server的请求。
					pullItable := NewITable()

					for k := range kv.ITable.SeqTable {
						// ClerkID的第63位为1，表明该请求来自于server而不是来自于clerk
						// 因此不需要进行同步。
						if k>>62 == 1 {
							continue
						}
						pullItable.SeqTable[k] = kv.ITable.SeqTable[k]
						pullItable.ReplyTable[k] = kv.ITable.ReplyTable[k]
					}
					reply = PullReply{DB: pullDB, Err: OK}
				}
			case "Merge":
				if op.Cfgnum < kv.config.Num {
					reply = MergeReply{Err: ErrLowerConfig}
					retry = true
				} else {
					args := op.Value.(MergeArgs)
					mergeDB := args.DB

					// 合并数据，完成数据迁移
					for k, v := range mergeDB {
						kv.Database[k] = v
					}

					// 如果一个请求已经在在另一个备份组被执行了，那就不应该再次被执行
					for k := range args.SeqTable {
						if args.SeqTable[k] >= kv.ITable.SeqTable[k] {
							kv.ITable.UpdateIdentifier(k, args.SeqTable[k], args.ReplyTable[k])
						}
					}
					kv.pullTasks.Remove(args.FromGID)
					reply = MergeReply{Err: OK}
					Debug(DServer, "[*] S%d#%d&%d MERGE: %+v. PULLTASKS:%d#%+v", kv.me, kv.gid, kv.config.Num, args, kv.pullTasks.Len(), kv.pullTasks.Export())
				}
			case "ReConfig":
				if op.Cfgnum < kv.config.Num+1 {
					reply = ReConfigReply{Err: ErrLowerConfig}
					retry = true
				} else {
					kv.pullTasks.Reset()
					args := op.Value.(ReConfigArgs)
					originCfg := kv.config
					// 其他的工作协程会并发的访问kv.config，因此加锁避免读写冲突。

					// 添加pull tasks
					pullList := make(map[int][]int)
					if originCfg.Num != 0 {
						for sid, gid := range args.Config.Shards {
							pullGID := originCfg.Shards[sid]
							if gid == kv.gid && pullGID != kv.gid {
								pullList[pullGID] = append(pullList[pullGID], sid)
							}
						}

						if len(pullList) > 0 {
							for gid, shards := range pullList {
								kv.pullTasks.Add(TaskStruct{
									Cfgnum:    args.Config.Num,
									ToGID:     gid,
									ToServers: originCfg.Groups[gid],
									Shards:    shards,
								})
							}
						}
					}
					Debug(DServer, "[*] S%d#%d&%d RECONFIG: %+v. PULLLIST:%+v", kv.me, kv.gid, kv.config.Num, args, pullList)
					reply = ReConfigReply{Err: OK}

					atomic.StoreInt64((*int64)(unsafe.Pointer(&kv.config.Num)), int64(args.Config.Num))
					kv.config.Groups = args.Config.Groups
					kv.config.Shards = args.Config.Shards

				}
			case "Get":
				if op.Cfgnum < kv.config.Num {
					reply = GetReply{Err: ErrLowerConfig}
					retry = true
				} else if kv.pullTasks.Contains(key2shard(op.Key.(string))) {
					reply = GetReply{Err: ErrMigrating}
					retry = true
				} else {
					reply = kv.Database.Get(op.Key.(string))
				}
			case "Put":
				if op.Cfgnum < kv.config.Num {
					reply = PutAppendReply{Err: ErrLowerConfig}
					retry = true
				} else if kv.pullTasks.Contains(key2shard(op.Key.(string))) {
					reply = PutAppendReply{Err: ErrMigrating}
					retry = true
				} else {
					reply = kv.Database.Put(op.Key.(string), op.Value.(string))
				}
			case "Append":
				if op.Cfgnum < kv.config.Num {
					reply = PutAppendReply{Err: ErrLowerConfig}
					retry = true
				} else if kv.pullTasks.Contains(key2shard(op.Key.(string))) {
					reply = PutAppendReply{Err: ErrMigrating}
					retry = true
				} else {
					reply = kv.Database.Append(op.Key.(string), op.Value.(string))
				}
			}

			Debug(DServer, "S%d#%d&%d FINISH REQ:%+v, REPLY:%+v", kv.me, kv.gid, kv.config.Num, op, reply)

			// 更新clerkID对应的Client的下一个待执行Op的Seq
			if !retry {
				kv.ITable.UpdateIdentifier(identifier.ClerkID, identifier.Seq+1, reply)
			}

			// 唤醒等待op执行结果的clerk协程。
			// 如果op.ServerID == kv.me说明该op是通过当前Server提交的，并且
			// 当applyMsg.Replay == false时说明该op是在server重启后提交的。
			//
			// 重启前提交的op需要被重放，但是不存在clerk协程等待server重启前提交的op。
			kv.OpReplys.SetAndBroadcast(Index(index), op, reply, op.ServerID == kv.me && !applyMsg.Replay)

			// 通知raft进行snapshot
			// 一定要在安装完快照之后才拍摄新的快照
			// 否则若在安装快照之前拍摄新的快照：当raft宕机恢复之后判断raft.statesize足够大了，此时数据库的状态为空！
			if kv.maxraftstate > -1 && kv.maxraftstate <= kv.rf.RaftStateSize() {
				Debug(DServer, "[*] S%d SNAPSHOT. CI:%d", kv.me, applyMsg.CommandIndex)
				kv.rf.Snapshot(applyMsg.CommandIndex, kv.Snapshot())
			}
		}
	}
}

// 拍摄快照
// 注意避免读写冲突
func (kv *ShardKV) Snapshot() []byte {
	snapshot := new(bytes.Buffer)
	e := labgob.NewEncoder(snapshot)
	if err := e.Encode(&kv.Database); err != nil {
		log.Fatalf("S%d fail to encode Database, err:%v\n", kv.me, err)
	}

	seqTable, replyTable := kv.ITable.Export(true)
	if err := e.Encode(seqTable); err != nil {
		log.Fatalf("S%d fail to encode SeqTable, err:%v\n", kv.me, err)
	}

	if err := e.Encode(replyTable); err != nil {
		log.Fatalf("S%d fail to encode ReplyTable, err:%v\n", kv.me, err)
	}

	if err := e.Encode(kv.config); err != nil {
		log.Fatalf("S%d fail to encode config, err:%v\n", kv.me, err)
	}

	tasks := kv.pullTasks.Export()
	if err := e.Encode(tasks); err != nil {
		log.Fatalf("S%d fail to encode tasks, err:%v\n", kv.me, err)
	}

	return snapshot.Bytes()
}

// 安装快照
// 注意避免读写冲突
func (kv *ShardKV) InstallSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&kv.Database); err != nil {
		log.Fatalf("S%d fail to decode database, err:%v\n", kv.me, err)
	}

	seqTable := make(map[int]int) // 记录每一个clerk的待提交的Op Sequence number
	replyTable := make(map[int]interface{})
	if err := d.Decode(&seqTable); err != nil {
		log.Fatalf("S%d fail to decode seqTable, err:%v\n", kv.me, err)
	}
	if err := d.Decode(&replyTable); err != nil {
		log.Fatalf("S%d fail to decode replyTable, err:%v\n", kv.me, err)
	}

	cfg := shardctrler.Config{}
	if err := d.Decode(&cfg); err != nil {
		log.Fatalf("S%d fail to decode config, err:%v\n", kv.me, err)
	}

	tasks := make(map[int]TaskStruct)
	if err := d.Decode(&tasks); err != nil {
		log.Fatalf("S%d fail to decode tasks, err:%v\n", kv.me, err)
	}

	// 安装快照前先清空原有的内容
	for id, seq := range seqTable {
		kv.ITable.UpdateIdentifier(id, seq, replyTable[id])
	}

	kv.pullTasks.Reset()
	for _, taskStruct := range tasks {
		kv.pullTasks.Add(taskStruct)
	}
	atomic.StoreInt64((*int64)(unsafe.Pointer(&kv.config.Num)), int64(cfg.Num))
	kv.config.Groups = cfg.Groups
	kv.config.Shards = cfg.Shards

	Debug(DServer, "S%d#%d&%d INSTALL SNAPSHOT DONE. DB:%+v, SEQ:%+v, REPLY:%+v, CFG:%+v", kv.me, kv.gid, kv.config.Num, kv.Database, kv.SeqTable, kv.ReplyTable, kv.config)
}

// 定时更新自己的config
// 从需要向其它的备份组拉取数据
// 将新的config和拉取到的数据封装成op交给Raft做备份，并等待其被执行。执行成功标识ReConfig完成。
func (kv *ShardKV) poll() {

	for {
		time.Sleep(time.Millisecond * 100)

		if kv.killed() {
			return
		}

		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		Debug(DServer, "S%d#%d&%d POLL. PULLTASK LEN:%d", kv.me, kv.gid, kv.config.Num, kv.pullTasks.Len())
		if kv.pullTasks.Len() == 0 {

			// 由于Query会超时，所以这里加上超时控制。
			var cfg shardctrler.Config
			originCfgNum := int(atomic.LoadInt64((*int64)(unsafe.Pointer(&kv.config.Num))))
			cfg = kv.mck.Query(originCfgNum + 1)
			Debug(DServer, "S%d#%d&%d QUERY CONFIG. OLDCFG:%d NEWCFG:%d.", kv.me, kv.gid, kv.config.Num, originCfgNum, cfg.Num)
			if cfg.Num == originCfgNum {
				continue
			}

			reconfigArg := ReConfigArgs{Config: cfg}
			op := Op{
				ServerID: kv.me,
				Kind:     "ReConfig",
				Value:    reconfigArg,
				Cfgnum:   cfg.Num,

				// 因此可以将备份组的gid，以及新config的编号作为唯一标识符。
				// Identifier中ClerkID的最高位为1，标识该请求来自于server而不是Clerk。
				ID: Identifier{
					ClerkID: 1<<62 | kv.gid,
					Seq:     cfg.Num,
				},
			}

			if ok, _ := kv.ITable.Executed(op.ID); ok {
				Debug(DServer, "S%d#%d&%d SEND RECONFIG FAILED. Executed", kv.me, kv.gid, kv.config.Num)
				continue
			}

			index, _, isLeader := kv.rf.Start(op)
			if !isLeader {
				Debug(DServer, "S%d#%d&%d SEND RECONFIG FAILED. NOT LEADER", kv.me, kv.gid, kv.config.Num)
				continue
			}
			Debug(DServer, "S%d#%d&%d SEND RECONFIG: %+v", kv.me, kv.gid, kv.config.Num, reconfigArg)

			// 等待Raft提交ReConfig请求并执行。
			// 如果ReConfig请求执行成功，说明ReConfig请求完成
			// 再来一遍，否则重新更新config。
			kv.WaitAndMatch(index, op)
		} else {
			tasks := kv.pullTasks.Export()
			Debug(DServer, "S%d#%d&%d PULL TASKS:%+v", kv.me, kv.gid, kv.config.Num, tasks)

			// 执行拉取协程，从gid标识的备份组拉取ts.Shards对应的数据
			for gid, taskStruct := range tasks {
				go func(gid int, ts TaskStruct) {
					pullargs := PullArgs{
						Shards:  ts.Shards,
						Cfgnum:  ts.Cfgnum,
						FromGID: kv.gid,
						ToGID:   gid,
					}

					Debug(DServer, "[*] S%d#%d&%d CALL `ShardKV.Pull`: %+v", kv.me, kv.gid, kv.config.Num, pullargs)
					reply := kv.pull(&pullargs, ts.ToServers)
					if reply == nil {
						return
					}

					mergeArgs := MergeArgs{
						FromGID:    gid,
						DB:         reply.DB,
						SeqTable:   reply.SeqTable,
						ReplyTable: reply.ReplyTable,
					}

					op := Op{
						ServerID: kv.me,
						Kind:     "Merge",
						Value:    mergeArgs,
						Cfgnum:   ts.Cfgnum,

						// 因此可以将被拉取备份组的gid，以及新config的编号作为唯一标识符。
						// Identifier中ClerkID的最高位为1，标识该请求来自于server而不是Clerk。
						ID: Identifier{
							ClerkID: 1<<62 | gid,
							Seq:     ts.Cfgnum,
						},
					}

					if ok, _ := kv.ITable.Executed(op.ID); ok {
						return
					}
					index, _, isLeader := kv.rf.Start(op)
					if !isLeader {
						return
					}

					// 等待Raft提交ReConfig请求并执行。
					// 如果ReConfig请求执行成功，说明ReConfig请求完成
					// 再来一遍，否则重新更新config。
					kv.WaitAndMatch(index, op)

				}(gid, taskStruct)
			}
		}
	}
}

// 向servers标识的备份组拉取shardID对应的切片
func (kv *ShardKV) pull(args *PullArgs, servers []string) *PullReply {

	for i := 0; i < len(servers); i++ {

		if kv.killed() {
			return nil
		}

		// reconfig 成功，无需再拉取
		cfgnum := int(atomic.LoadInt64((*int64)(unsafe.Pointer(&kv.config.Num))))
		if cfgnum != args.Cfgnum {
			return nil
		}

		srv := kv.make_end(servers[i])
		retCh := make(chan bool, 1) // 这里必须是带缓冲的，为了能够让工作协程顺利退出
		reply := new(PullReply)

		go func() {
			retCh <- srv.Call("ShardKV.Pull", args, reply)
		}()

		select {
		case <-retCh:
		case <-time.After(time.Millisecond * 30):
		}

		if reply.Err == OK {
			return reply
		}
	}
	return nil
}
