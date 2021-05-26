package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type Identifier struct {
	ClerkID int
	Seq     int
}

type ITable map[int]int

func (itable ITable) NextIdentifier(clerkID int) Identifier {
	seq := itable[clerkID]
	itable[clerkID] = seq + 1
	return Identifier{
		ClerkID: clerkID,
		Seq:     seq,
	}
}

// 如果ii已经被执行过了，Executed返回true
func (itable ITable) Executed(i Identifier) bool {
	return i.Seq < itable[i.ClerkID]
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key     string
	ClerkID int
}

type GetReply struct {
	Err   Err
	Value string
}
