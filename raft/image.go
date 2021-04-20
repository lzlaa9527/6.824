package raft

// Image记录server状态的一个镜像
// 如果有工作协程发生某些事件需要更新server的状态，就向update中发送更新server状态的函数
//
// server状态更新后，可能会替换Image且原来的Image会失效；
type Image struct {

	// update用来通知ticker更新server状态
	update chan func(i *Image)
	did    chan signal
	// 当server状态改变时，ticker协程会关闭done，通知工作协程放弃手头的工作
	// 因此每个server的多个工作协程都要通过监听done判断server的状态是否改变
	//
	// ticker 只会关闭done一次；因为ticker协程关闭done之后会申请新的rf.Image
	done chan signal

	// server 状态的镜像
	CurrentTerm int
	State       int
	VotedFor    int
	*Raft
}

func (rf *Raft) GetImage() Image {
	// rf.mu.RLock()
	// defer rf.mu.RUnlock()
	i := *rf.Image
	return i
}

// 当在工作协程中发生如下事件时，工作协程会通知ticker协程执行act()更新server的状态并执行相应动作
// 事件：收到新leader的AP RPC，投出自己的选票，获得足够的选票，发现自己的term落后了；
//
// ticker 只会关闭done一次， 因此Cancel是并发安全的
func (image Image) Update(act func(i *Image)) bool {

	// 判断Image是否早已被取消
	select {
	case <-image.done:
		return false
	default:
		break
	}

	select {
	case <-image.done:
		return false
	case image.update <- act: // 发送更新server状态的函数，通知 ticker 取消当前的Image
		<-image.did
		return true
	}
}

// 工作协程通过判断done是否关闭，确定server状态是否发生改变
func (image Image) Done() bool {
	select {
	case <-image.done: // 已关闭
		return true
	default:
		return false
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.CurrentTerm, rf.State == LEADER
}
