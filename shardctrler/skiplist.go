package shardctrler

import (
	"fmt"
	"math/rand"
)

const (
	SkipListP = 0.5
	MaxLevel  = 16 // 最大索引级数; 0级索引标识原始链表
)

type node struct {
	val      interface{}
	key      int
	level    int     // 所在的层次
	forwards []*node // 每层后继节点指针; 位于第二层的节点，第一层、第二次都有后继节点
}

func newNode(k int, v interface{}, level int) *node {
	return &node{
		val:      v,
		key:      k,
		level:    level,
		forwards: make([]*node, level),
	}
}

type SkipList struct {
	head   *node // 头结点，便于查找
	level  int   // 跳表的当前层数
	length int   // 跳表的长度
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:   newNode(-1, -1, MaxLevel),
		level:  1,
		length: 0,
	}
}

// Length 返回SkipList原始链表长度
func (s *SkipList) Length() int {
	return s.length
}

// Level 返回SkipList层级
func (s *SkipList) Level() int {
	return s.level
}

// 理论来讲，一级索引中元素个数应该占原始数据的 50%，二级索引中元素个数占 25%，三级索引12.5% ，一直到最顶层。
// 因为这里每一层的晋升概率是 50%。对于每一个新插入的节点，都需要调用 randomLevel 生成一个合理的层数。
// 该 randomLevel 方法会随机生成 0~MAX_LEVEL-1 之间的数，且 ：
//        50%的概率返回 1——第0层，原始结点
//        25%的概率返回 2——第1层，索引节点
//      12.5%的概率返回 3 ...
func randomLevel() int {
	level := 1
	for rand.Float64() < SkipListP && level < MaxLevel {
		level++
	}
	return level
}

// Insert 向SkipList中插入结点，并返回插入后的结点
// 如果插入重复关键字的结点，返回error
func (s *SkipList) Insert(k int, v interface{}) (*node, error) {
	cur := s.head             // 查找插入位置
	path := [MaxLevel]*node{} // 查找路径

	for i := MaxLevel - 1; i >= 0; i-- {
		for cur.forwards[i] != nil { // 查找第i层的结点
			if cur.forwards[i].key == k {
				return cur.forwards[0], fmt.Errorf("duplicated key: %d\n", k)
			}

			if cur.forwards[i].key > k {
				path[i] = cur
				break
			}
			cur = cur.forwards[i] // 查找后续结点
		}
		// 新结点应该插入在cur与cur.forwards[i]之间
		path[i] = cur

	}

	level := randomLevel()
	node := newNode(k, v, level)

	// 构建索引结点间的链接
	// 由于新结点的层次为level所以，在1~level层中需要将新结点插入到path[i]与path[i].forwards[i]之间
	for i := 0; i < level; i++ {
		nxt := path[i].forwards[i]
		path[i].forwards[i] = node
		node.forwards[i] = nxt
	}

	if level > s.level {
		s.level = level
	}

	s.length++
	return node, nil
}

// Search 返回SkipList中关键字为k的结点，如果不存在返回nil
func (s *SkipList) Search(k int) *node {
	if s.length == 0 {
		return nil
	}

	cur := s.head
	for i := s.level - 1; i >= 0; i-- {
		for cur.forwards[i] != nil {
			if cur.forwards[i].key == k {
				return cur.forwards[0] // 找到原始链表(第0层)的目标节点
			} else if cur.forwards[i].key > k {
				break
			}
			cur = cur.forwards[i]
		}
	}
	return nil
}

// Delete 删除SkipList中关键字为k的结点
// 如果SkipList为空返回error
func (s *SkipList) Delete(k int) (*node, error) {
	if s.length == 0 {
		return nil, fmt.Errorf("empth SkipList\n")
	}

	cur := s.head
	path := [MaxLevel]*node{} // 被删除结点在各层的前驱结点

	for i := s.level - 1; i >= 0; i-- {
		for cur.forwards[i] != nil && cur.forwards[i].key < k {
			cur = cur.forwards[i]
		}
		path[i] = cur
	}

	// 存在目标结点
	if cur.forwards[0] != nil && cur.forwards[0].key == k {
		cur = cur.forwards[0] // cur指向被删除结点
		for i := cur.level - 1; i >= 0; i-- {

			// 删除cur结点之后第i层不存在任何索引了
			if path[i] == s.head && cur.forwards[i] == nil {
				s.level = i
			}
			path[i].forwards[i] = cur.forwards[i] // 短路被删除结点
		}
		s.length--
		return cur, nil
	}
	return nil, fmt.Errorf("nonexistent key:%d\n", k)
}

// Print 打印SkipList
func (s *SkipList) Print() {
	fmt.Printf("total level: %d\n", s.level)
	for i := s.level - 1; i >= 0; i-- {
		fmt.Printf("level-%d: ", i)
		cur := s.head
		for cur != nil {
			fmt.Printf("(%d, %v) ", cur.key, cur.val)
			cur = cur.forwards[i]
		}
		fmt.Println()
	}
}

// Next 找到链表中n结点的后继节点，如果尾元结点不满足要求从首元结点开始查找
// 如果查找一圈也没有合适的结点就
func (s *SkipList) Next(n *node) *node {
	if n == nil {
		return nil
	}

	if s.Length() <= 1 {
		return nil
	}

	nxt := n.forwards[0]
	if nxt == nil {
		nxt = s.head.forwards[0]
	}

	return nxt
}
