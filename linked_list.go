package mr

import (
	"fmt"
	"time"
)

type WorkerNode struct {
	next *WorkerNode
	prev *WorkerNode

	WorkerNumber                                int
	EmployementStatus                           bool
	TotalNumberOfHeartBeatsSent                 int
	TotalNumberOfHeartBeatsSentSinceEmployement int
	NumberOfJobsCompleted                       int
	TimeStamp                                   time.Time
	JobAllocatedLocation                        string
}

func NewNode(wn int) *WorkerNode {
	new := WorkerNode{nil, nil, wn, false, 0, 0, 0, time.Now(), ""}
	return &new
}

func (c *Coordinator) GetLatestValue() int {
	tmp := c.head
	if tmp == nil {
		return 0
	}
	for ; tmp.next != nil; tmp = tmp.next {
	}
	return tmp.WorkerNumber
}

func (c *Coordinator) InsertNode() int {
	bar := c.GetLatestValue() + 1
	if c.head == nil {
		c.head = NewNode(bar)
		return bar
	}
	foo := NewNode(c.GetLatestValue())
	tmp := c.head
	for ; tmp.next != nil; tmp = tmp.next {
	}
	tmp.next = foo
	foo.prev = tmp
	return bar
}

func (c *Coordinator) DeleteWorker(wn int) {
	tmp := c.head

	if c.head.WorkerNumber == wn && c.head.next == nil {
		c.head = nil
	} else if c.head.WorkerNumber == wn && c.head.next != nil {
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

	for ; tmp.next != nil && tmp.WorkerNumber != wn; tmp = tmp.next {
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

func ToDelete(tmp *WorkerNode) bool {
	currTime := time.Now()

	x := currTime.Sub(tmp.TimeStamp)
	if x > 9 {
		fmt.Println("Deleted node with wn : ", tmp.WorkerNumber)
		return true
	}
	return false
}

func (c *Coordinator) HouseKeeping() {
	tmp := c.head
	for ; tmp.next != nil; tmp = tmp.next {
		if ToDelete(tmp) {
			if tmp.next == nil && tmp.prev != nil {
				back := tmp.prev
				back.next = nil
				tmp.prev = nil
			} else if tmp.next == nil && tmp.prev == nil {
				c.head = nil
			} else if tmp.prev == nil && tmp.next != nil {
				tmp.next = nil
			} else {
				back := tmp.prev
				front := tmp.next
				back.next = front
				front.prev = back
				tmp.next = nil
				tmp.prev = nil
			}
		}
	}
}
