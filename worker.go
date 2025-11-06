package mr

import (
	//	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Global Variables
	workerNumber := CallToInitialize()
	employementStatus := false
	NumberOfJobsCompleted := 0
	TotalNumberOfHeartBeatsSent := 0
	TotalNumberOfHeartBeatsSentSinceEmployement := 0
	locationToBeRead := ""
	TheNumberOfReduceTasks := 0
	typeOfJobAssigned := 0

	foo := time.NewTicker(time.Second * 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			// hearbear logic
			case <-foo.C:
				fmt.Println("Is it entering here")
				numberOfReduceTasks, NumberOfHeartBeatsSent, NumberOfHeartBeatsSentSinceEmployement, TypeOfJobAssigned, AssigningJob, locationToBeReadFrom := actualHeartbeatLogic(workerNumber, NumberOfJobsCompleted, TotalNumberOfHeartBeatsSent, TotalNumberOfHeartBeatsSentSinceEmployement, employementStatus)
				if AssigningJob {
					employementStatus = true
					locationToBeRead = locationToBeReadFrom
					TotalNumberOfHeartBeatsSentSinceEmployement = NumberOfHeartBeatsSentSinceEmployement
					typeOfJobAssigned = TypeOfJobAssigned
				}
				TotalNumberOfHeartBeatsSent = NumberOfHeartBeatsSent
				TheNumberOfReduceTasks = numberOfReduceTasks
				// main kelsa
			default:
				// map task
				if employementStatus && typeOfJobAssigned == 1 {
					fmt.Println("Enters here")
					intermediate := []KeyValue{}
					FileContent := RetriveFileContent("map-files/" + locationToBeRead)
					kva := mapf(locationToBeRead, string(FileContent))
					intermediate = append(intermediate, kva...)

					sort.Sort(ByKey(intermediate))

					name := fmt.Sprintf("inter-files/mr-map-inter-%d-%d", workerNumber, ihash(string(FileContent))%TheNumberOfReduceTasks)
					dst, err3 := os.Create(name)
					if err3 != nil {
						panic(err3)
					}
					enc := json.NewEncoder(dst)
					err := enc.Encode(intermediate)
					if err != nil {
						panic(err)
					}
				} else if employementStatus && typeOfJobAssigned == 2 {
				}
				NumberOfJobsCompleted = NumberOfJobsCompleted + 1
				employementStatus = false
			}
		}
	}()
	wg.Wait()
}

func RetriveFileContent(FilePath string) []byte {
	FileInstance, err := os.Open(FilePath)
	if err != nil {
		panic(err)
	}
	defer FileInstance.Close()
	FileContent, err := io.ReadAll(FileInstance)
	if err != nil {
		panic(err)
	}
	return FileContent
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

func actualHeartbeatLogic(workerNumber, NumberOfJobsCompleted, TotalNumberOfHeartBeatsSent, TotalNumberOfHeartBeatsSentSinceEmployement int, employementStatus bool) (int, int, int, int, bool, string) {
	ToSend := HeartbeatSyn{workerNumber, employementStatus, NumberOfJobsCompleted, TotalNumberOfHeartBeatsSent, TotalNumberOfHeartBeatsSentSinceEmployement, time.Now()}
	Reply := HeartbearAck{}

	ok := call("Coordinator.HeartBeatCoordinator", &ToSend, &Reply)
	if ok {
		fmt.Println("Heratbeat complete : Total Number of Heartbeats completed - ", Reply.TotalNumberOfHeartBeatsSent)
	} else {
		fmt.Println("Something went wrong while sending the hearbeat to the coordinator ")
	}
	return Reply.NumberOfReduceTasks, Reply.TotalNumberOfHeartBeatsSent, Reply.TotalNumberOfHeartBeatsSentSinceEmployement, Reply.TypeOfJob, Reply.AssigningJob, Reply.JobAllocatedLocation
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
