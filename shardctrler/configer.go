package shardctrler

import (
	"fmt"
	"sort"
)

// 所有实现shards分配的struct都要是实现该接口
type Configer interface {

	// 将一组由servers组成的备份组，加入当前的集群
	Join(servers map[int][]string)

	// 从集群中删除gids指定的备份组
	Leave(gids []int)

	// 将shardID指定的shard交给gid指定的备份组负责管理
	Move(shardID int, gid int)
	Export(num int) Config
}

// 采用双向优先队列管理所有的备份组，根据备份组所负责的shards数量进行排序
type defaultConfiger struct {
	sd       SortedDeque      // 有序双端队列
	shards   [NShards]int     // shards -> gid
	groups   map[int][]string // gid -> servers[]
	assigned map[int][]int    // gid -> shards[]
}

func newDefaultConfiger() *defaultConfiger {
	return &defaultConfiger{
		sd:       NewSortedDeque(),
		shards:   [10]int{},
		groups:   make(map[int][]string),
		assigned: make(map[int][]int),
	}
}

func (dc *defaultConfiger) Export(num int) Config {
	groupCopy := make(map[int][]string)
	for gid, group := range dc.groups {
		cp := make([]string, len(group))
		copy(cp, group)
		groupCopy[gid] = cp
	}
	return Config{
		Num:    num,
		Shards: dc.shards,
		Groups: groupCopy,
	}
}

func (dc *defaultConfiger) Join(servers map[int][]string) {

	gids := []int{}
	for gid := range servers {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	for i := 0; i < len(gids); i++ {
		gid := gids[i]

		if _, ok := dc.groups[gid]; ok {
			panic(fmt.Sprintf("replica group:%d already exist\n", gid))
		}

		dc.groups[gid] = servers[gid]

		// 首个备份组需要负责所有的shards
		if len(dc.groups) == 1 {
			for i := 0; i < NShards; i++ {
				dc.shards[i] = gid
				dc.assigned[gid] = append(dc.assigned[gid], i)
			}
			dc.sd.Insert(group{gid: gid, nShards: NShards})
			continue
		}

		n := NShards / len(dc.groups) // 新加入的备份组最少需要分配n个shards

		// if n <= 0 {
		// 	panic(fmt.Sprintf("too much replica groups:%d than nshards:%d\n", len(dc.groups), NShards))
		// }

		c := 0
		res := []int{}
		for dc.sd.Len() > 0 {
			gid, nshards := dc.sd.Peek()
			d := min(n-c, nshards-n)

			// 从gid负责的shards中取出n个shards交给新加入的备份组管理
			dc.sd.Update(gid, -d)
			res = append(res, dc.assigned[gid][nshards-d:]...)
			dc.assigned[gid] = dc.assigned[gid][:nshards-d]

			c += d
			if c == n {
				break
			}
		}

		dc.sd.Insert(group{gid: gid, nShards: n})
		dc.assigned[gid] = append(dc.assigned[gid], res...)
		for i := 0; i < len(res); i++ {
			dc.shards[res[i]] = gid
		}
	}
}

func (dc *defaultConfiger) Leave(gids []int) {
	for _, gid := range gids {

		// 将gid负责的shards分配给其他的备份组负责
		delete(dc.groups, gid)
		res := dc.assigned[gid] // 需要重新分配的shards
		delete(dc.assigned, gid)
		dc.sd.Remove(gid)

		// 最后一个备份组也被移除
		if len(dc.groups) == 0 {
			for i := 0; i < NShards; i++ {
				dc.shards[i] = 0
			}
			return
		}

		// 需要重新分配的shards数量
		c := len(res)

		// 去除一个备份组之后，其余的备份组最多需要n个shards
		n := (NShards + len(dc.groups) - 1) / len(dc.groups)

		for dc.sd.Len() > 0 {
			gid, nShards := dc.sd.Tail()
			d := min(n-nShards, c)

			// 取出d个shards交给gid标识的备份组管理
			dc.sd.Update(gid, +d)
			tmp := res[c-d:]
			for i := 0; i < len(tmp); i++ {
				dc.shards[tmp[i]] = gid
			}
			dc.assigned[gid] = append(dc.assigned[gid], tmp...)
			res = res[:c-d]

			c -= d
			if c == 0 {
				break
			}
		}
	}

	// 所有的备份组均已宕机
	if len(dc.groups) == 0 {
		for i := 0; i < NShards; i++ {
			dc.shards[i] = 0
		}
	}

}

func (dc *defaultConfiger) Move(shardID int, gid int) {
	if _, ok := dc.groups[gid]; !ok {
		panic(fmt.Sprintf("move to nonexistent replica group"))
	}

	// 将shardID从原来的备份组中去除
	oldGroup := dc.shards[shardID]

	if oldGroup == 0 {
		panic(fmt.Sprintf("can't move shard form replica group:%d\n", oldGroup))
	}

	for i := 0; i < len(dc.assigned[oldGroup]); i++ {
		if dc.assigned[oldGroup][i] == shardID {

			tmp := dc.assigned[oldGroup][:i]
			tmp = append(tmp, dc.assigned[oldGroup][i+1:]...)
			dc.assigned[oldGroup] = tmp
		}
	}
	dc.sd.Update(oldGroup, -1)

	// 将shardID加入到gid标识的备份组
	dc.shards[shardID] = gid
	dc.assigned[gid] = append(dc.assigned[gid], shardID)
	dc.sd.Update(gid, +1)
}

func (dc *defaultConfiger) statistic() map[int]int {
	m := make(map[int]int)
	for i, ints := range dc.assigned {
		m[i] = len(ints)
	}
	return m
}

func (dc *defaultConfiger) String() string {
	return fmt.Sprintf("%+v\n", dc.assigned)
}
