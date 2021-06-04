// 一致性哈希
// 一致性哈希的目的是，当存在结点数量变更时尽可能少的迁移数据。
// 详见: https://blog.csdn.net/kefengwang/article/details/81628977

package shardctrler

import (
	"container/list"
	"fmt"
	"sort"
	"strconv"
)

const VirtualCopies = 1 << 16 // 物理节点至虚拟节点的复制倍数

type hashcode int

// 32位的 Fowler-Noll-Vo 哈希算法
// https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
func fnvhash(key string) hashcode {
	const p = 16777619
	hash := 2166136261
	for i := 0; i < len(key); i++ {
		hash = (hash ^ int(key[i])) * p
	}

	hash += hash << 13
	hash ^= hash >> 7
	hash += hash << 3
	hash ^= hash >> 17
	hash += hash << 5

	if hash < 0 {
		hash = -hash
	}

	return hashcode(hash)
}

type request struct {
	randStr string // 用于计算哈希的随机字符串
	hashcode
	shardID int
}

// 哈希环，所有的(虚拟)结点、请求都会被映射到HashRing上的2^32个slot之一。(使得请求分布尽可能的均匀)
// 增加物理结点时，会创建 VirtualCopies 个虚拟节点，删除物理节点时会溢出对应的虚拟节点；
// 每个虚拟节点会负责一段哈希区间的请求；落在虚拟节点负责范围内的请求，交给对应的物理节点处理。
type HashRing struct {
	vnodeList *SkipList             // 存储所有的虚拟节点 key=hashcode, value=vnode
	pnodes    map[int]*physicalNode // pid ==> pnode
	shards    map[int]*virtualNode  // shardID ==> pid

	unBalancedShards *list.List // 尚未平衡分配的shard，调用join、leave时会对其进行分配
}

type virtualNode struct {
	*physicalNode
	shards *list.List // 负责的shard
}

type physicalNode struct {
	prefix string // 隶属于该物理节点的虚拟节点的公共前缀
	pid    int
	vnodes map[hashcode]*virtualNode // 属于物理节点的虚拟节点，hashcode ==> vnode
}

func NewHashring() *HashRing {
	return &HashRing{
		vnodeList:        NewSkipList(),
		pnodes:           make(map[int]*physicalNode),
		shards:           make(map[int]*virtualNode),
		unBalancedShards: list.New(),
	}
}

// Join添加物理节点及其虚拟节点，并重新分配与新增加的虚拟节点相邻的后续虚拟节点所管理的shards
func (hs *HashRing) Join(pid int) error {

	if _, ok := hs.pnodes[pid]; ok {
		return fmt.Errorf("duplicated pid:%d\n", pid)
	}

	pnode := &physicalNode{vnodes: make(map[hashcode]*virtualNode), pid: pid, prefix: randstring(20) + Itoa(pid) + "#"}
	hs.pnodes[pid] = pnode // 插入物理节点

	// 创建虚拟节点
	for i := 0; i < VirtualCopies; i++ {
		vnodeHashcode := fnvhash(pnode.prefix + "#" + Itoa(i))
		vnode := &virtualNode{
			physicalNode: pnode,
			shards:       list.New(),
		}

		n, err := hs.vnodeList.Insert(int(vnodeHashcode), vnode)

		// 如果当前虚拟节点与其它的虚拟节点产生了哈希冲突，就忽略该结点
		if err != nil {
			fmt.Printf("hash collision, key:%v\n", pnode.prefix+"#"+Itoa(i))
			continue
		}

		pnode.vnodes[vnodeHashcode] = vnode

		// 第一个物理节点无需rebalance操作
		if len(hs.pnodes) == 1 {
			continue
		}

		// 重新分配新插入虚拟节点vnode的后继节点nxtVNode所管理的shards
		// 所有shardHashcode <= vnodeHashcode 的shard划分给vnode负责
		nv := hs.vnodeList.Next(n)
		nxtVNode := nv.val.(*virtualNode)

		for e := nxtVNode.shards.Front(); e != nil; {
			nxt := e.Next()
			shard := e.Value.(*request)
			if shard.hashcode <= vnodeHashcode {
				vnode.shards.PushBack(shard)
				hs.shards[shard.shardID] = vnode

				// fmt.Printf("move (shardID:%d, shardID hashcode:%d) from (pid:%d, vnode hashcode:%d) to (pid:%d, vnode hashcode:%d);\n", shard.shardID, shard.hashcode, nxtVNode.pid, nv.key, pid, vnodeHashcode)

				nxtVNode.shards.Remove(e)
			}
			e = nxt

		}
	}

	return hs.ReBalance()
}

// Leave 删除物理节点及其虚拟节点，并将被删除的虚拟节点所管理的shards分配的到相邻的后续虚拟结点
func (hs *HashRing) Leave(pid int) error {
	if _, ok := hs.pnodes[pid]; !ok {
		return fmt.Errorf("can't leave nonexistent pid:%d\n", pid)
	}

	if len(hs.pnodes) == 1 {
		return fmt.Errorf("can't delete the only pnod, pid:%d\n", pid)
	}

	pnode := hs.pnodes[pid]

	// 将被删除虚拟节点负责的shards重新分配给相邻的后继虚拟节点
	for vnodeHashcode, vnode := range pnode.vnodes {
		if hs.vnodeList.Length() <= 1 {
			return fmt.Errorf("hs.vnodeList.Len()=%d\n", hs.vnodeList.Length())
		}

		// 删除虚拟节点
		n, err := hs.vnodeList.Delete(int(vnodeHashcode))
		if err != nil {
			return err
		}

		// 找到被删除虚拟结点的后继虚拟结点啊
		nxt := hs.vnodeList.Next(n)
		nxtVNode := nxt.val.(*virtualNode)
		for p := vnode.shards.Front(); p != nil; p = p.Next() {
			shard := p.Value.(*request)
			hs.shards[shard.shardID] = nxtVNode

			// fmt.Printf("move (shardID:%d, shardID hashcode:%d) from (pid:%d, vnode hashcode:%d) to (pid:%d, vnode hashcode:%d);\n", shard.shardID, vnode.pid, vnodeHashcode, shard.hashcode, nxtVNode.pid, nxt.key)
		}
		nxtVNode.shards.PushBackList(vnode.shards)
	}

	// 删除该物理节点
	delete(hs.pnodes, pid)

	return hs.ReBalance()
}

// Move 将shard从原来的虚拟节点shards集合中删除；
// 再将其临时分配给给一个临时虚拟节点，并将其记录在hs.unBalancedShards链表中；
// 当指向Join、Leave时再重新分配hs.unBalancedShards中的shard。
func (hs *HashRing) Move(shardID, pid int) error {

	vnode, ok := hs.shards[shardID]
	// 将shard从原来的虚拟节点shards集合中删除；
	if ok {
		for e := vnode.shards.Front(); e != nil; e = e.Next() {
			if e.Value.(*request).shardID == shardID {
				vnode.shards.Remove(e)
				break
			}
		}
	}

	pnode, ok := hs.pnodes[pid]
	if !ok {
		return fmt.Errorf("nonexistent pnode, pid:%d\n", pid)
	}

	// 创建一个临时的且属于pid的虚拟节点来管理shard
	tmpVNode := &virtualNode{
		physicalNode: pnode,
		shards:       nil,
	}
	tmpVNode.shards = list.New()
	unBalancedRequest := &request{
		randStr: randstring(20) + "#" + Itoa(shardID),
		shardID: shardID,
	}
	unBalancedRequest.hashcode = fnvhash(unBalancedRequest.randStr)
	tmpVNode.shards.PushBack(unBalancedRequest)
	hs.shards[shardID] = tmpVNode

	hs.unBalancedShards.PushBack(unBalancedRequest) // 记录在hs.unBalancedShards链表中；
	return nil
}

// ReBalance 重新分配hs.unBalancedShards中的所有shards
func (hs *HashRing) ReBalance() error {
	if hs.unBalancedShards.Len() == 0 {
		return nil
	}

	if hs.vnodeList.Length() == 0 {
		return fmt.Errorf("no virtual nodes, hs.vnodeList.Len()=%d\n", hs.vnodeList.Length())
	}

	// 将shard分配给对应的虚拟节点
	for e := hs.unBalancedShards.Front(); e != nil; {
		shard := e.Value.(*request)

		// 先向vnodeList中插入一个临时结点，再从vnodeList查找到该临时结点，就能够快速确定该shard所属的虚拟节点
		n, err := hs.vnodeList.Insert(int(shard.hashcode), nil)

		nv := hs.vnodeList.Next(n)
		nxtVnode := nv.val.(*virtualNode) // shard所属的虚拟节点
		nxtVnode.shards.PushBack(shard)
		hs.shards[shard.shardID] = nxtVnode

		// 需要删除插入成功的临时节点
		if err == nil {
			hs.vnodeList.Delete(int(shard.hashcode))
		}

		// fmt.Printf("assign (shardID:%d, hashCode:%d) to (pid:%d, vnodeHashCode:%d)\n", shard.shardID, shard.hashcode, nxtVnode.pid, nv.key)

		nxt := e.Next()
		hs.unBalancedShards.Remove(e) // 删除已经分配的shard
		e = nxt
	}
	return nil
}

func (hs *HashRing) String() {

	vn := 0
	for _, pn := range hs.pnodes {
		vn += len(pn.vnodes)
	}
	fmt.Printf("virtual nodes cound:%d\n", vn)

	pnodes := make(map[int][]int, len(hs.pnodes))

	for shardID, vnode := range hs.shards {
		pid := vnode.pid
		pnodes[pid] = append(pnodes[pid], shardID)
	}

	for pid, shards := range pnodes {
		sort.Ints(pnodes[pid])
		fmt.Printf("pid:%d, shards count:%d/%d\n", pid, len(shards), len(hs.shards))
	}
}

func Itoa(k int) string {
	return strconv.Itoa(k)
}
