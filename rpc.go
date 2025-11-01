package mr

import (
	"os"
	"strconv"
)

// Hello : worker -> master: assigns a unique worker number
type Hello struct {
	HelloWorld string
}

// ACKing : worker -> master : confirms it recieved the hello rpc
type InitializeWorker struct {
	AssignedWorkerNumber int
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
