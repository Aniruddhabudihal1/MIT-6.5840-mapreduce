package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
	cr "github.com/go-co-op/gocron/v2"
)

type Coordinator struct {
	head *WorkerNode
	MapQueueHead *MappingQueueNode
	reduceQueueHead *ReduceQueueNode
}

// start a thread that listens for RPCs from worker.go
// It is through this that, worker communicates with the Coordinator
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) NewWorker(argument *Hello, response *InitializeWorker) error {
	var foo string
	foo = "hello world"
	if strings.Compare(argument.HelloWorld, foo) == 0 {
		response.AssignedWorkerNumber = c.InsertNode()
		return nil
	} else {
		return errors.New("Something went wrong while communicating with the newly made worker\nmake sure the hello message being sent is accurate")
	}
}

func (c *Coordinator) HeartBeatCoordinator(argument *HeartbeatSyn, resp *HeartbearAck) error {
	wn := argument.WorkerNumber
	nodeInstance := c.GetWorkerDetails(wn)
	nodeInstance.TotalNumberOfHeartBeatsSent = nodeInstance.TotalNumberOfHeartBeatsSent + 1
	nodeInstance.TimeStamp = argument.Timestamp
	fmt.Println("Total Number of heartbeats sent : ", nodeInstance.TotalNumberOfHeartBeatsSent, "from worker number : ", argument.WorkerNumber)

	// TODO: Implement logic to provide job to the worker and then increment TotalNumberOfHeartBeatsSentSinceEmployemnt as well
	// TODO: If a job gets over, should increment numbeofjobscompleted

	resp.WorkerNumber = nodeInstance.WorkerNumber
	resp.EmployementStatus = nodeInstance.EmployementStatus
	resp.TotalNumberOfHeartBeatsSent = nodeInstance.TotalNumberOfHeartBeatsSent
	resp.NumberOfJobsCompleted = nodeInstance.NumberOfJobsCompleted
	resp.TotalNumberOfHeartBeatsSentSinceEmployement = nodeInstance.TotalNumberOfHeartBeatsSentSinceEmployement
	return nil
}

func ExecuteCronJobs(){
	start := time.Duration(3 * time.Second)
	end := time.Duration(9 * time.Second)
	gocron.DurationRandomJob(start, end).
}

// create a Coordinator. main/mrcoordinator.go calls this function. nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.server()

	return &c
}

// main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

/*
// an example RPC handler.

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
*/
