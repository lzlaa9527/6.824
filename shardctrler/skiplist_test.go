package shardctrler

import (
	"fmt"
	"testing"
)

func TestSkipList(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(95, "leo")
	fmt.Printf("after %s\n", "insert(95, leo)")
	sl.Print()
	fmt.Println()

	sl.Insert(88, "jack")
	fmt.Printf("after %s\n", "insert(88, jack)")
	sl.Print()
	fmt.Println()

	sl.Insert(100, "lily")
	fmt.Printf("after %s\n", "insert(100, lily)")
	sl.Print()
	fmt.Println()

	res := sl.Search(88)
	fmt.Printf("search(%d): %d %v\n", 88, res.key, res.val)
	fmt.Println()

	sl.Delete(95)
	fmt.Printf("after %s\n", "delete(95)")
	sl.Print()
	fmt.Println()
}
