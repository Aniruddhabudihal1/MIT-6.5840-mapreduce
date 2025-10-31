package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments and reply for an RPC.

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

func coordinatorSock() string {
	// The temp files go into this directory
	s := "5840-mr-coordinator"
	s += strconv.Itoa(os.Getuid())
	return s
}
