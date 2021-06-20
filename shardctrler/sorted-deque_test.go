package shardctrler

import (
	"fmt"
	"testing"
)

func TestSortedDeque_Insert(t *testing.T) {
	sd := NewSortedDeque()

	sd.Insert(group{
		gid:     1,
		nShards: 10,
	})

	sd.Insert(group{
		gid:     2,
		nShards: 4,
	})

	sd.Insert(group{
		gid:     6,
		nShards: 7,
	})

	sd.Insert(group{
		gid:     3,
		nShards: 1,
	})

	sd.Insert(group{
		gid:     5,
		nShards: 7,
	})

	for sd.Len() > 0 {
		gid, nShards := sd.Tail()
		fmt.Printf("gid:%d, nshards:%d\n", gid, nShards)
		sd.Remove(gid)
	}
}

func TestSortedDeque_Update(t *testing.T) {
	sd := NewSortedDeque()

	sd.Insert(group{
		gid:     1,
		nShards: 10,
	})

	sd.Insert(group{
		gid:     2,
		nShards: 4,
	})

	sd.Insert(group{
		gid:     6,
		nShards: 7,
	})

	sd.Insert(group{
		gid:     3,
		nShards: 1,
	})

	sd.Insert(group{
		gid:     5,
		nShards: 7,
	})

	sd.Update(1, 5)
	sd.Update(3, 6)
	sd.Update(2, 15)
	sd.Update(6, 3)

	for sd.Len() > 0 {
		gid, nShards := sd.Tail()
		fmt.Printf("gid:%d, nshards:%d\n", gid, nShards)
		sd.Remove(gid)
	}
}
