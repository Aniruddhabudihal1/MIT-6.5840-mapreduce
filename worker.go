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
func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

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

	IntermediateDataToBeSent := []KeyValue{}

	// Global Variables
	workerNumber := CallToInitialize()
	employementStatus := false
	NumberOfJobsCompleted := 0
	TotalNumberOfHeartBeatsSent := 0
	TotalNumberOfHeartBeatsSentSinceEmployement := 0
	MaplocationToBeRead := ""
	reduceLocationToBeRead := ""
	finalFinalOutputLocation := ""

	typeOfJobAssigned := 0

	foo := time.NewTicker(time.Second * 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			// hearbear logic
			case <-foo.C:
				reduceJobAllocatedFile, NumberOfHeartBeatsSent, NumberOfHeartBeatsSentSinceEmployement, TypeOfJobAssigned, AssigningJob, MaplocationToBeReadFrom, FinalOutputLocation := actualHeartbeatLogic(workerNumber, NumberOfJobsCompleted, TotalNumberOfHeartBeatsSent, TotalNumberOfHeartBeatsSentSinceEmployement, employementStatus)
				if AssigningJob {
					employementStatus = true
					MaplocationToBeRead = MaplocationToBeReadFrom
					TotalNumberOfHeartBeatsSentSinceEmployement = NumberOfHeartBeatsSentSinceEmployement
					typeOfJobAssigned = TypeOfJobAssigned
					finalFinalOutputLocation = FinalOutputLocation
					reduceLocationToBeRead = reduceJobAllocatedFile
				}
				TotalNumberOfHeartBeatsSent = NumberOfHeartBeatsSent
				// main kelsa
			default:
				// map task
				if employementStatus && typeOfJobAssigned == 1 {
					fmt.Println("Starting Map Job")

					FileContent := RetriveFileContent("map-files/" + MaplocationToBeRead)
					kv := mapf(MaplocationToBeRead, string(FileContent))
					IntermediateDataToBeSent = append(IntermediateDataToBeSent, kv...)
					name := fmt.Sprintf("inter-files/mr-map-inter-%d-%d", workerNumber, ihash(string(FileContent))%100)
					dst, err3 := os.Create(name)
					if err3 != nil {
						panic(err3)
					}
					enc := json.NewEncoder(dst)
					err := enc.Encode(IntermediateDataToBeSent)
					if err != nil {
						panic(err)
					}
					// reduce task
				} else if employementStatus && typeOfJobAssigned == 2 {
					fmt.Println("Starting the reduce job")
					fmt.Println("Reading ", reduceLocationToBeRead)
					input, err := os.OpenFile(reduceLocationToBeRead, os.O_CREATE|os.O_RDWR, 0o644)
					if err != nil {
						panic(err)
					}
					fmt.Println("final output file ", finalFinalOutputLocation)
					output1, err := os.OpenFile(finalFinalOutputLocation, os.O_CREATE|os.O_RDWR, 0o644)
					if err != nil {
						panic(err)
					}

					Intermed := []KeyValue{}
					dec := json.NewDecoder(input)

					for {
						var KVinstance KeyValue
						fmt.Println("hello")
						if err := dec.Decode(&Intermed); err != nil {
							break
						}
						fmt.Println(Intermed)
						Intermed = append(Intermed, KVinstance)
					}
					sort.Sort(ByKey(Intermed))

					i := 0
					fmt.Println(Intermed)
					for i < len(Intermed) {
						j := i + 1
						for j < len(Intermed) && Intermed[j].Key == Intermed[i].Key {
							j++
						}
						values := []string{}
						for k := i; k < j; k++ {
							values = append(values, Intermed[k].Value)
						}
						actualOutput := reducef(Intermed[i].Key, values)

						amountWritten, err := fmt.Fprintf(output1, "%v %v\n", Intermed[i].Key, actualOutput)
						if err != nil {
							panic(err)
						}
						fmt.Println(amountWritten)

						i = j
					}
					output1.Close()
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

func actualHeartbeatLogic(workerNumber, NumberOfJobsCompleted, TotalNumberOfHeartBeatsSent, TotalNumberOfHeartBeatsSentSinceEmployement int, employementStatus bool) (string, int, int, int, bool, string, string) {
	ToSend := HeartbeatSyn{workerNumber, employementStatus, NumberOfJobsCompleted, TotalNumberOfHeartBeatsSent, TotalNumberOfHeartBeatsSentSinceEmployement, time.Now()}
	Reply := HeartbearAck{}

	ok := call("Coordinator.HeartBeatCoordinator", &ToSend, &Reply)
	if ok {
		fmt.Println("Heratbeat complete : Total Number of Heartbeats completed - ", Reply.TotalNumberOfHeartBeatsSent)
	} else {
		fmt.Println("Something went wrong while sending the hearbeat to the coordinator ")
	}
	return Reply.ReduceJobAllocatedFile, Reply.TotalNumberOfHeartBeatsSent, Reply.TotalNumberOfHeartBeatsSentSinceEmployement, Reply.TypeOfJob, Reply.AssigningJob, Reply.MapJobAllocatedLocation, Reply.FinalOutputLocation
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
