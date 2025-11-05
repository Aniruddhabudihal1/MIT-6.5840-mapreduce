package mr

import (
	"os"
	"strconv"
	"time"
)

// Hello : worker -> master: Hello world thats all
type Hello struct {
	HelloWorld string
}

// InitializeWorker: master -> worker : assigns a unique worker number to the worker instance
type InitializeWorker struct {
	AssignedWorkerNumber int
}

type HeartbeatSyn struct {
	WorkerNumber                                int
	EmployementStatus                           bool
	NumberOfJobsCompleted                       int
	TotalNumberOfHeartBeatsSent                 int
	TotalNumberOfHeartBeatsSentSinceEmployement int
	Timestamp                                   time.Time
}

type HeartbearAck struct {
	WorkerNumber                                int
	AssigningJob                                bool
	TotalNumberOfHeartBeatsSent                 int
	TotalNumberOfHeartBeatsSentSinceEmployement int
	JobAllocatedLocation                        string
	NumberOfReduceTasks                         int
}

func coordinatorSock() string {
	// The temp files go into this directory
	s := "5840-mr-temp-files"
	s += strconv.Itoa(os.Getuid())
	return s
}

/*
// example to show how to declare the arguments and reply for an RPC.
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
*/
