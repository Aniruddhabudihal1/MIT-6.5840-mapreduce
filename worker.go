package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var wg sync.WaitGroup

	workerNumber := CallToInitialize()
	employementStatus := false
	NumberOfJobsCompleted := 0
	TotalNumberOfHeartBeatsSent := 0
	TotalNumberOfHeartBeatsSentSinceEmployement := 0

	foo := time.NewTicker(time.Second * 3)
	tickerChan := make(chan any)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-tickerChan:
				fmt.Println("Meanwhile Some other logic too be implemented here")
			case <-foo.C:
				actualHeartbeatLogic(workerNumber, NumberOfJobsCompleted, TotalNumberOfHeartBeatsSent, TotalNumberOfHeartBeatsSentSinceEmployement, employementStatus)
			}
		}
	}()
	wg.Wait()
}

func CallToInitialize() int {
	ToSend := Hello{"hello world"}

	reply := InitializeWorker{}

	ok := call("Coordinator.NewWorker", &ToSend, &reply)
	if ok {
		fmt.Println("The worker number assigned to this worker is : ", reply.AssignedWorkerNumber)
		return reply.AssignedWorkerNumber
	} else {
		fmt.Printf("while trying to Initialize Worker by communicating with the server the call failed!\n")
		return -1
	}
}

func actualHeartbeatLogic(workerNumber, NumberOfJobsCompleted, TotalNumberOfHeartBeatsSent, TotalNumberOfHeartBeatsSentSinceEmployement int, employementStatus bool) {
	ToSend := HeartbeatSyn{workerNumber, employementStatus, NumberOfJobsCompleted, TotalNumberOfHeartBeatsSent, TotalNumberOfHeartBeatsSentSinceEmployement, time.Now()}
	Reply := HeartbearAck{}

	ok := call("Coordinator.HeartBeatCoordinator", &ToSend, &Reply)
	if ok {
		fmt.Println("Heratbeat complete : Total Number of Heartbeats completed - ", Reply.TotalNumberOfHeartBeatsSent)
	} else {
		fmt.Println("Something went wrong while sending the hearbeat to the coordinator ")
	}
}

// send an RPC request to the coordinator, wait for the response usually returns true returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	httpconn, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer httpconn.Close()

	err = httpconn.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

/*
// example function to show how to make an RPC call to the coordinator the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	args.X = 99

	reply := ExampleReply{}

	// send the RPC request, wait for the reply the "Coordinator.Example" tells the receiving server that we'd like to call the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v got from the coordinator\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
*/
