package mr

type WorkerNode struct {
	next *WorkerNode
	prev *WorkerNode

	WorkerNumber                                int
	EmployementStatus                           bool
	TotalNumberOfHeartBeatsSent                 int
	TotalNumberOfHeartBeatsSentSinceEmployement int
	NumberOfJobsCompleted                       int
}

func NewNode(wn int) *WorkerNode {
	new := WorkerNode{nil, nil, wn, false, 0, 0, 0}
	return &new
}

func (c *Coordinator) GetLatestValue() int {
	tmp := c.head
	if tmp == nil {
		return 1
	}
	for ; tmp.next != nil; tmp = tmp.next {
	}
	x := tmp.WorkerNumber
	return x + 1
}

func (c *Coordinator) InsertNode() int {
	if c.head == nil {
		c.head = NewNode(c.GetLatestValue())
		return c.GetLatestValue()
	}
	foo := NewNode(c.GetLatestValue())
	tmp := c.head
	for ; tmp.next != nil; tmp = tmp.next {
	}
	tmp.next = foo
	return c.GetLatestValue()
}

func (c *Coordinator) DeleteBasedOnWn(wn int) {
	tmp := c.head

	if tmp.WorkerNumber == wn {
		c.head = tmp.next
		tmp.next = nil
		return
	}
	for ; tmp.next != nil && tmp.WorkerNumber == wn; tmp = tmp.next {
	}
	if tmp.next != nil {
		foo := tmp.prev
		bar := tmp.next
		foo.next = bar
		bar.prev = foo
		tmp.next = nil
		tmp.prev = nil
	} else {
		foo := tmp.prev
		foo.next = nil
		tmp.prev = nil
	}
}

func (c *Coordinator) GetWorkerDetails(wn int) *WorkerNode {
	tmp := c.head

	for ; tmp.next != nil && tmp.WorkerNumber == wn; tmp = tmp.next {
	}
	return tmp
}

func (c *Coordinator) IncrementNumberOfHeartbeat(wn int) int {
	node := c.GetWorkerDetails(wn)
	ret := node.TotalNumberOfHeartBeatsSent + 1
	return ret
}

func (c *Coordinator) IncrementNumberOfHeartbeatSinceJob(wn int) int {
	node := c.GetWorkerDetails(wn)
	ret := node.TotalNumberOfHeartBeatsSentSinceEmployement + 1
	return ret
}

func (c *Coordinator) SetHeartbeatToZero(wn int) {
	node := c.GetWorkerDetails(wn)
	node.TotalNumberOfHeartBeatsSentSinceEmployement = 0
}

func (c *Coordinator) ToggleEmployementStatus(wn int) {
	node := c.GetWorkerDetails(wn)
	if node.EmployementStatus == false {
		node.EmployementStatus = true
	} else {
		node.EmployementStatus = false
	}
}

func (c *Coordinator) IncrementJobsDone(wn int) int {
	node := c.GetWorkerDetails(wn)
	ret := node.NumberOfJobsCompleted + 1
	return ret
}
