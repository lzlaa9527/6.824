package kvraft

import (
	"fmt"
	"testing"
)

type Node struct {
	ct, vf int
	done   chan struct{}
}

func Test(t *testing.T) {
	n := Node{
		ct:   1,
		vf:   1,
		done: make(chan struct{}),
	}
	ch := make(chan func())
	go func() {
		f := <-ch
		f()
	}()

	var f func()
	f = func() {
		n2 := &Node{
			ct:   2,
			done: make(chan struct{})}
		n = *n2
	}
	ch <- f

	fmt.Printf("%v", n)

}
