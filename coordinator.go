package mr

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
)

type Coordinator struct {
	head                            *WorkerNode
	MapQueueHead                    *MappingQueueNode
	ReduceQueueHead                 *ReduceQueueNode
	MapDirName                      string
	InterDirName                    string
	FinalOutPutLocationOutput       string
	InputFiles                      []string
	InsertionIntoMapQueueDone       bool
	InsertionIntoTheReduceQueueDone bool
	MapWorkDone                     bool
	ReduceWorkIsDone                bool
	numberOfPopsForMapTasks         int
}

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

	if c.InsertionIntoMapQueueDone == false {
		for i := range c.InputFiles {
			ExtractContent(c.InputFiles[i])
		}
		c.AddingToQueue()
		fmt.Println("Insertion into the map queue Done")
		c.InsertionIntoMapQueueDone = true
	}
	resp.WorkerNumber = nodeInstance.WorkerNumber
	if c.IsMapQueueEmpty() && !c.InsertionIntoTheReduceQueueDone {
		fmt.Println("Map work done")
		c.MapWorkDone = true
		if c.InsertionIntoTheReduceQueueDone == false {
			interrnames, err := ListNames(c.InterDirName)
			if err != nil {
				panic(err)
			}
			for i := 0; i < NumberOfFiles(c.InterDirName); i++ {
				foo := NewReduceTask(c.InterDirName+"/"+interrnames[i], c.FinalOutPutLocationOutput)
				c.InsertIntoReduceQueue(foo)
			}
			c.InsertionIntoTheReduceQueueDone = true
			fmt.Println("Insertion into the reduce queue done")
		}
	}

	if c.IsReduceQueueEmpty() {
		c.ReduceWorkIsDone = true
	}

	if argument.EmployementStatus == false {
		nodeInstance.EmployementStatus = true
	}

	if !c.MapWorkDone {
		nodeInstance.TotalNumberOfHeartBeatsSentSinceEmployement += 1
		resp.AssigningJob = true
		jobLoc := c.PopMapTask()
		c.numberOfPopsForMapTasks++
		nodeInstance.MapJobAllocatedLocation = jobLoc.inputFilePath
		resp.MapJobAllocatedLocation = nodeInstance.MapJobAllocatedLocation
		resp.TypeOfJob = 1
		nodeInstance.EmployementStatus = false
	} else if c.MapWorkDone {
		fmt.Println("Enters for reduce job")
		resp.AssigningJob = true
		resp.TypeOfJob = 2
		foo1 := c.PopReduceTask()
		fmt.Println("Input location being sent to the worker is ", foo1.inputFilePath)
		fmt.Println("Ouput location being sent to the worker is ", foo1.outputFilePath)
		resp.FinalOutputLocation = foo1.outputFilePath
		resp.ReduceJobAllocatedFile = foo1.inputFilePath
		nodeInstance.EmployementStatus = false
	} else {
		resp.AssigningJob = false
		resp.TypeOfJob = 0
	}

	resp.TotalNumberOfHeartBeatsSent = nodeInstance.TotalNumberOfHeartBeatsSent
	resp.TotalNumberOfHeartBeatsSentSinceEmployement = nodeInstance.TotalNumberOfHeartBeatsSentSinceEmployement
	resp.FinalOutputLocation = c.FinalOutPutLocationOutput
	return nil
}

func NumberOfFiles(filePath string) int {
	files, _ := os.ReadDir(filePath)
	return len(files)
}

// create a Coordinator. main/mrcoordinator.go calls this function. nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.server()

	c.MapDirName = "map-files"
	c.InterDirName = "inter-files"
	c.FinalOutPutLocationOutput = "mr-map-final-output"
	c.InputFiles = files
	c.InsertionIntoMapQueueDone = false
	c.InsertionIntoTheReduceQueueDone = false
	c.MapWorkDone = false
	c.numberOfPopsForMapTasks = 0
	err1 := os.MkdirAll(c.MapDirName, 0o755)
	if err1 != nil {
		panic(err1)
	}
	err2 := os.MkdirAll(c.InterDirName, 0o755)
	if err2 != nil {
		panic(err2)
	}

	_, err3 := os.Create(c.FinalOutPutLocationOutput)
	if err3 != nil {
		panic(err3)
	}

	return &c
}

// This breaks down the main file and pushes the tasks to the mapping queue
func ExtractContent(fileName string) {
	fileInstance, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		log.Fatal("Something went wrong ! \nCheck if the file exists")
	}

	var offset int64
	offset = 128000
	var i int64
	i = 0
	for ; ; i++ {
		err := extraction(fileName, fileInstance, offset, i)
		if err != nil {
			break
		}
	}
}

func extraction(filename string, FileContent *os.File, offset int64, i int64) error {
	_, err1 := FileContent.Seek(offset*i, 0)
	if err1 != nil {
		panic(err1)
	}

	name := fmt.Sprintf("map-files/mr-map-task-%d-%s", i, filename)
	dst, err3 := os.Create(name)
	if err3 != nil {
		panic(err3)
	}
	_, err4 := io.CopyN(dst, FileContent, offset)

	if errors.Is(err4, io.EOF) || errors.Is(err4, io.ErrUnexpectedEOF) {
		return err4
	}

	return nil
}

func (c *Coordinator) AddingToQueue() {
	base := "/home/ani/Documents/6.5840/src/main/map-files/"
	inter := "/home/ani/Documents/6.5840/src/main/inter-files/"
	x, err := ListNames(base)
	if err != nil {
		panic(err)
	}
	for i := range x {
		foo := NewMapTask(x[i], inter)
		c.InsertIntoMapQueue(foo)
	}
}

func ListNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out, nil
}

// main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}
