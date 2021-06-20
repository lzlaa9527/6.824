package shardctrler

import "container/heap"

// 有序双端队列
type SortedDeque struct {
	minh *minheap // 大顶堆
	maxh *maxheap // 小顶堆
}

func NewSortedDeque() SortedDeque {
	return SortedDeque{
		minh: newMinheap(),
		maxh: newMaxheap(),
	}
}

func (sd SortedDeque) Len() int {
	return sd.maxh.Len()
}

func (sd SortedDeque) Insert(g group) {

	heap.Push(sd.maxh, g)
	heap.Push(sd.minh, g)
}

func (sd SortedDeque) Remove(gid int) (nshards int) {

	i2 := sd.maxh.position[gid]
	heap.Remove(sd.maxh, i2)

	i1 := sd.minh.position[gid]
	return heap.Remove(sd.minh, i1).(group).nShards
}

func (sd SortedDeque) Update(gid, nShards int) {
	i1 := sd.minh.position[gid]

	sd.minh.groups[i1].nShards += nShards
	heap.Fix(sd.minh, i1)

	i2 := sd.maxh.position[gid]
	sd.maxh.groups[i2].nShards += nShards
	heap.Fix(sd.maxh, i2)
}

func (sd SortedDeque) Peek() (gid, nShards int) {
	group := sd.maxh.groups[0]
	return group.gid, group.nShards
}

func (sd SortedDeque) Tail() (gid, nShards int) {
	group := sd.minh.groups[0]
	return group.gid, group.nShards
}

type group struct {
	gid     int
	nShards int // shards数量
}

// 实现小顶堆的类型
type minheap struct {
	groups   []group     // 堆结点中存储的是复制组负责管理的shards数量
	position map[int]int // 记录每个备份组在堆中的结点下标，是为了能够删除、修改指定的备份组
}

func newMinheap() *minheap {
	return &minheap{
		groups:   make([]group, 0),
		position: make(map[int]int),
	}
}

func (is *minheap) Len() int {
	return len(is.groups)
}

func (is *minheap) Less(i, j int) bool {
	if is.groups[i].nShards == is.groups[j].nShards {
		return is.groups[i].gid < is.groups[j].gid
	}
	return is.groups[i].nShards < is.groups[j].nShards
}

func (is *minheap) Swap(i, j int) {
	igid, jgid := is.groups[i].gid, is.groups[j].gid
	is.groups[i], is.groups[j] = is.groups[j], is.groups[i]
	is.position[igid], is.position[jgid] = is.position[jgid], is.position[igid]
}

func (is *minheap) Push(x interface{}) {
	g := x.(group)
	is.position[g.gid] = is.Len()
	is.groups = append(is.groups, x.(group))
}

func (is *minheap) Pop() interface{} {
	var g group
	is.groups, g = is.groups[:len(is.groups)-1], is.groups[len(is.groups)-1]
	delete(is.position, g.gid)
	return g
}

// 实现大顶堆的类型
type maxheap struct {
	*minheap
}

func newMaxheap() *maxheap {
	return &maxheap{newMinheap()}
}

func (is2 maxheap) Less(i, j int) bool {
	if is2.groups[i].nShards == is2.groups[j].nShards {
		return is2.groups[i].gid < is2.groups[j].gid
	}
	return is2.groups[i].nShards > is2.groups[j].nShards
}
