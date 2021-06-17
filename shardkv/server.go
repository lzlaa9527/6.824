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
	mu           sync.Mutex
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
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	curCfgnum := atomic.LoadInt64((*int64)(unsafe.Pointer(&kv.config.Num)))
	Debug(DServer, "[*] S%d#%d&%d RECEIVE `Get` OP:%+v", kv.me, kv.gid, curCfgnum, args)

	if args.Cfgnum < int(curCfgnum) || args.Cfgnum == 0 {
		reply.Err = ErrWrongConfig
		return
	}

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

	if kv.ITable.Executed(op.ID) {
		ret := kv.ITable.GetCacheReply(op.ID.ClerkID)
		reply.Err = ret.(GetReply).Err
		reply.Value = ret.(GetReply).Value
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
		reply.Err = ret.(GetReply).Err
		reply.Value = ret.(GetReply).Value
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	kv.mu.Lock()
	curCfgnum := kv.config.Num
	kv.mu.Unlock()
	Debug(DServer, "[*] S%d#%d&%d RECEIVE `PutAppend` OP:%+v", kv.me, kv.gid, curCfgnum, args)

	if args.Cfgnum < curCfgnum || args.Cfgnum == 0 {
		reply.Err = ErrWrongConfig
		return
	}

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

	if kv.ITable.Executed(op.ID) {
		ret := kv.ITable.GetCacheReply(op.ID.ClerkID)
		reply.Err = ret.(PutAppendReply).Err
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

	kv.mu.Lock()
	curCfgnum := kv.config.Num
	kv.mu.Unlock()

	Debug(DServer, "[*] S%d#%d&%d RECEIVE `Pull` OP:%+v", kv.me, kv.gid, curCfgnum, args)

	if args.Cfgnum > curCfgnum {
		reply.Err = ErrWrongConfig
		return
	}

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

	if kv.ITable.Executed(op.ID) {
		Debug(DServer, "[*] S%d#%d&%d RECEIVE `Pull` OP:%+v", kv.me, kv.gid, curCfgnum, args)

		ret := kv.ITable.GetCacheReply(op.ID.ClerkID)
		reply.Err = ret.(PullReply).Err
		reply.DB = ret.(PullReply).DB

		tmp := kv.ITable.Export(false)
		reply.SeqTable = tmp.SeqTable
		reply.ReplyTable = tmp.ReplyTable
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
		reply.Err = ret.(PullReply).Err
		reply.DB = ret.(PullReply).DB

		tmp := kv.ITable.Export(false)
		reply.SeqTable = tmp.SeqTable
		reply.ReplyTable = tmp.ReplyTable
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

	kv.ITable = NewITable()
	kv.OpReplys = NewOpReplays()
	kv.Database = make(Database)

	go kv.applier()
	go kv.poll()

	Debug(DServer, "[*] S%d#%d start.", me, kv.gid)
	return kv
}

// applier是唯一改变的server状态的工作协程，主要执行如下工作：
// 1. applier安装raft提交的快照，重建Database，以及ITable。
// 2. applier协程用来执行被raft提交的op更新数据库状态，并且唤醒等待该op的clerk协程；
// 3. 如果raft的statesize达到maxstatesize，就通知raft拍摄快照
func (kv *ShardKV) applier() {
	for applyMsg := range kv.applyCh {
		if kv.killed() {
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
			if kv.ITable.Executed(identifier) {
				reply := kv.ITable.GetCacheReply(op.ID.ClerkID)
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
					reply = PullReply{Err: ErrWrongConfig}
					retry = true
				} else {

					pullArgs := op.Value.(PullArgs)
					pullDB := make(Database)
					for k, v := range kv.Database {
						shardID := key2shard(k)
						for _, sid := range pullArgs.ShardsID {
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

			case "ReConfig":
				if op.Cfgnum != kv.config.Num+1 {
					reply = ReConfigReply{Err: ErrWrongConfig}
					retry = true
				} else {
					args := op.Value.(ReConfigArgs)
					mergeDB := args.DB

					kv.mu.Lock()
					kv.config = args.Config // 更新config
					kv.mu.Unlock()

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
					reply = ReConfigReply{Err: OK}
				}
			case "Get":
				if op.Cfgnum != kv.config.Num {
					reply = GetReply{Err: ErrWrongConfig}
					retry = true
				} else {
					reply = kv.Database.Get(op.Key.(string))
				}
			case "Put":
				if op.Cfgnum != kv.config.Num {
					reply = PutAppendReply{Err: ErrWrongConfig}
					retry = true
				} else {
					reply = kv.Database.Put(op.Key.(string), op.Value.(string))
				}
			case "Append":
				if op.Cfgnum != kv.config.Num {
					reply = PutAppendReply{Err: ErrWrongConfig}
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
				Debug(DServer, "[*] S%d SNAPSHOT.", kv.me)
				kv.rf.Snapshot(applyMsg.CommandIndex, kv.Snapshot())
			}
		}
	}
}

// 拍摄快照
func (kv *ShardKV) Snapshot() []byte {
	snapshot := new(bytes.Buffer)
	e := labgob.NewEncoder(snapshot)
	if err := e.Encode(&kv.Database); err != nil {
		log.Fatalf("S%d fail to encode database, err:%v\n", kv.me, err)
	}

	tmp := kv.ITable.Export(true)
	if err := e.Encode(tmp.SeqTable); err != nil {
		log.Fatalf("S%d fail to encode SeqTable, err:%v\n", kv.me, err)
	}

	if err := e.Encode(tmp.ReplyTable); err != nil {
		log.Fatalf("S%d fail to encode ReplyTable, err:%v\n", kv.me, err)
	}

	if err := e.Encode(kv.config); err != nil {
		log.Fatalf("S%d fail to encode config, err:%v\n", kv.me, err)
	}

	// fmt.Printf("[*] C%d, snapshot:\ndatabase:%v\nitable:%v\n", kv.me, kv.DB, kv.ITable.SeqTable)
	return snapshot.Bytes()
}

// 安装快照
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
	for id, seq := range seqTable {
		kv.ITable.UpdateIdentifier(id, seq, replyTable[id])
	}

	kv.mu.Lock()
	if err := d.Decode(&kv.config); err != nil {
		log.Fatalf("S%d fail to decode config, err:%v\n", kv.me, err)
	}
	kv.mu.Unlock()

	Debug(DServer, "S%d#%d&%d INSTALL SNAPSHOT DONE. DB:%+v, SEQ:%+v, REPLY:%+v, CFG:%+v", kv.me, kv.gid, kv.config.Num, kv.Database, kv.SeqTable, kv.ReplyTable, kv.config)
}

// 定时更新自己的config
// 从需要向其它的备份组拉取数据
// 将新的config和拉取到的数据封装成op交给Raft做备份，并等待其被执行。执行成功标识ReConfig完成。
func (kv *ShardKV) poll() {

loop:
	for {
		time.Sleep(time.Millisecond * 100)

		if kv.killed() {
			return
		}

		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		kv.mu.Lock()
		originCfg := kv.config
		kv.mu.Unlock()

		cfg := kv.mck.Query(originCfg.Num + 1)

		if cfg.Num == originCfg.Num {
			continue
		}

		// 记录需要从各个group拉取的shards
		fetchList := make(map[int][]int)

		if cfg.Num > 1 {
			for shardID, gid := range cfg.Shards {
				if gid == kv.gid && originCfg.Shards[shardID] != kv.gid {
					fetchList[originCfg.Shards[shardID]] = append(fetchList[originCfg.Shards[shardID]], shardID)
				}
			}
		}

		Debug(DServer, "S%d#%d&%d FETCH LIST: %+v", kv.me, kv.gid, originCfg.Num, fetchList)

		mergeDB := make(Database)
		mergeITable := NewITable()
		pullData := []*PullReply{}

		// 从其它备份组拉取数据
		if len(fetchList) > 0 {
			wg := sync.WaitGroup{}

			// 拉取fetchList中记录的shards
			for gid, shards := range fetchList {
				if servers, ok := originCfg.Groups[gid]; ok {
					args := &PullArgs{
						ShardsID: shards,
						Cfgnum:   cfg.Num,
						FromGID:  kv.gid,
						ToGID:    gid,
					}
					wg.Add(1)
					go func(agrs *PullArgs, originCfgnum int, servers []string) {
						defer wg.Done()
						Debug(DServer, "S%d#%d&%d FETCH:%v FROM %v.", kv.me, kv.gid, originCfgnum, args.ShardsID, args.FromGID)

						reply := kv.pull(args, originCfgnum, servers)

						Debug(DServer, "S%d#%d&%d FETCH:%v FROM %v. reply:%+v", kv.me, kv.gid, originCfgnum, args.ShardsID, args.FromGID, servers, reply)
						if reply != nil {
							pullData = append(pullData, reply)
						}
					}(args, originCfg.Num, servers)
				}
			}
			wg.Wait()

			// 没有数据没有拉取成功
			if len(pullData) != len(fetchList) {
				continue loop
			}

			for _, pd := range pullData {
				// 将拉取到的数据组合起来
				for k, v := range pd.DB {
					mergeDB[k] = v
				}

				for k := range pd.SeqTable {
					if pd.SeqTable[k] >= mergeITable.SeqTable[k] {
						mergeITable.SeqTable[k] = pd.SeqTable[k]
						mergeITable.ReplyTable[k] = pd.ReplyTable[k]
					}
				}
			}
		}

		// 将reconfig的请求交由Raft做备份
		args := ReConfigArgs{
			Config:     cfg,
			DB:         mergeDB,
			SeqTable:   mergeITable.SeqTable,
			ReplyTable: mergeITable.ReplyTable,
		}

		op := Op{
			ServerID: kv.me,
			Kind:     "ReConfig",
			Value:    args,
			Cfgnum:   cfg.Num,

			// 因此可以将备份组的gid，以及新config的编号作为唯一标识符。
			// Identifier中ClerkID的最高位为1，标识该请求来自于server而不是Clerk。
			ID: Identifier{
				ClerkID: 1<<62 | kv.gid,
				Seq:     cfg.Num,
			},
		}

		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			continue
		}
		// 等待Raft提交ReConfig请求并执行。
		// 如果ReConfig请求执行成功，说明ReConfig请求完成
		// 再来一遍，否则重新更新config。
		kv.WaitAndMatch(index, op)

	}
}

// 向servers标识的备份组拉取shardID对应的切片
func (kv *ShardKV) pull(args *PullArgs, originCfgnum int, servers []string) *PullReply {

	for {

		for i := 0; i < len(servers); i++ {

			if kv.killed() {
				return nil
			}

			// reconfig 成功，无需再拉取

			kv.mu.Lock()
			cfgnum := kv.config.Num
			kv.mu.Unlock()

			if cfgnum != originCfgnum {
				return nil
			}

			Debug(DServer, "[*] S%d#%d&%d PULL CFG:%d, SHARDS:%+v FROM SRVS:%v", kv.me, kv.gid, originCfgnum, args.Cfgnum, args.ShardsID, servers)

			srv := kv.make_end(servers[i])
			retCh := make(chan bool, 1) // 这里必须是带缓冲的，为了能够让工作协程顺利退出
			reply := new(PullReply)

			go func() {
				retCh <- srv.Call("ShardKV.Pull", args, reply)
			}()

			var ok bool
			select {
			case ok = <-retCh:
			case <-time.After(raft.HEARTBEAT * 10):
				ok = false
			}

			// Call返回false或者定时器到期，表明请求超时
			if !ok {
				time.Sleep(raft.HEARTBEAT * 1)
			} else {

				switch reply.Err {
				case OK:
					return reply
				case ErrWrongLeader:
				case ErrWrongConfig:
					time.Sleep(raft.HEARTBEAT * 1)
				}
			}
		}
		// 如果所有的server都不是leader，那就等待300ms
		time.Sleep(raft.HEARTBEAT * 1)
	}
}
