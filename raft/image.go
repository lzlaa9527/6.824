package raft

// Image 记录某一时刻 server 的状态；工作协程可以通过监听 Image 实例中的 done 管道
// 判断 server 的状态是否改变，工作协程（在 update 函数中）通过关闭 done 管道以通知
// 所有监听当前 Image 实例的工作协程放弃当前的任务；当工作协程需要更新 server 的状态，
// 工作协程可以将 server 状态更新的方式通过 Image 实例中的update管道发送过去；ticker
// 协程通过监听 update 管道，来执行 update 函数更新server的状态；当 ticker 协程执行
// 完 update 函数之后，向 did 管道发送信号通知等待的工作协程；
type Image struct {
	update chan func(i *Image)
	did    chan signal
	done   chan signal

	// server 状态
	CurrentTerm int
	State       int
	VotedFor    int
	// Image所属的Raft实例
	*Raft
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


