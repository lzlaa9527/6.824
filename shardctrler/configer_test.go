package shardctrler

import (
	"reflect"
	"testing"
)

func TestDefaultConfiger_Join(t *testing.T) {
	dc := defaultConfiger{
		sd:       NewSortedDeque(),
		shards:   [10]int{},
		groups:   make(map[int][]string),
		assigned: make(map[int][]int),
	}

	tests := []struct {
		name   string
		params map[int][]string
		result map[int]int
	}{
		{"Join(1)", map[int][]string{1: []string{"a", "b", "c"}}, map[int]int{1: 10}},
		{"Join(2)", map[int][]string{2: []string{"x", "y", "z"}}, map[int]int{1: 5, 2: 5}},
		{"Join(3)", map[int][]string{3: []string{"i", "j", "k"}}, map[int]int{1: 3, 2: 4, 3: 3}},
		{"Join(4)", map[int][]string{4: []string{"q", "w", "e"}}, map[int]int{1: 3, 2: 2, 3: 3, 4: 2}},
		{"Join(5)", map[int][]string{5: []string{"o", "p", "q"}}, map[int]int{1: 2, 2: 2, 3: 2, 4: 2, 5: 2}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dc.Join(test.params)
			t.Logf(dc.String() + "\n")
			if res := dc.statistic(); !reflect.DeepEqual(res, test.result) {
				t.Fatalf("expected:%v, got:%v\n", res, test.result)
			}
		})
	}
}

func TestDefaultConfiger_Leave(t *testing.T) {

	dc := defaultConfiger{
		sd:       NewSortedDeque(),
		shards:   [10]int{},
		groups:   make(map[int][]string),
		assigned: make(map[int][]int),
	}

	args := map[int][]string{1: []string{"a", "b", "c"}, 2: []string{"x", "y", "z"},
		3: []string{"i", "j", "k"}, 4: []string{"q", "w", "e"}, 5: []string{"o", "p", "q"}}

	dc.Join(args)

	tests := []struct {
		name   string
		input  []int
		output map[int]int
	}{
		{"Leave(2)", []int{2}, map[int]int{1: 3, 3: 3, 4: 2, 5: 2}},
		{"Leave(3)", []int{3}, map[int]int{1: 3, 4: 4, 5: 3}},
		{"Leave(4)", []int{4}, map[int]int{1: 5, 5: 5}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dc.Leave(test.input)
			t.Logf(dc.String() + "\n")
			if res := dc.statistic(); !reflect.DeepEqual(res, test.output) {
				t.Fatalf("expected:%v, got:%v\n", res, test.output)
			}
		})
	}
}

func TestDefaultConfiger_Move(t *testing.T) {
	dc := defaultConfiger{
		sd:       NewSortedDeque(),
		shards:   [10]int{},
		groups:   make(map[int][]string),
		assigned: make(map[int][]int),
	}

	args := map[int][]string{1: []string{"a", "b", "c"}, 2: []string{"x", "y", "z"},
		3: []string{"i", "j", "k"}, 4: []string{"q", "w", "e"}, 5: []string{"o", "p", "q"}}

	dc.Join(args)

	tests := []struct {
		name string

		shardID int
		gid     int

		output map[int]int
	}{
		{"Move(0,1)", 0, 1, map[int]int{1: 2, 2: 2, 3: 2, 4: 2, 5: 2}},
		{"Leave(0,5)", 0, 5, map[int]int{1: 1, 2: 2, 3: 2, 4: 2, 5: 3}},
		{"Leave(9,2)", 9, 2, map[int]int{1: 1, 2: 3, 3: 2, 4: 2, 5: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dc.Move(test.shardID, test.gid)
			t.Logf(dc.String() + "\n")
			t.Logf("%v\n", dc.statistic())
			if res := dc.statistic(); !reflect.DeepEqual(res, test.output) {
				t.Fatalf("expected:%v, got:%v\n", res, test.output)
			}
		})
	}
}
