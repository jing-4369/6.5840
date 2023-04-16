package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

var Mapf func(string, string) []KeyValue
var Reducef func(string, []string) string

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	Mapf = mapf
	Reducef = reducef

	callAskWork()
}

func callAskWork() {

	args := AskWorkArgs{os.Getpid()}

	reply := AskWorkReply{}

	ok := call("Coordinator.AskWork", &args, &reply)
	if ok {
		switch reply.taskType {
		case MapTaskType: // map work
			doMapTask(reply.fileName, reply.nReduce, reply.taskId)

		case ReduceTaskType: // reduce work
		}
	} else {
		time.Sleep(time.Second)
		callAskWork()
	}

}
func callTaskFinished(taskType int, taskId int, fileNames map[int]string) {
	args := TaskFinishedArgs{}

	args.taskType = taskType
	args.taskId = taskId
	args.fileNames = fileNames
	reply := TaskFinishedReply{}

	err := call("Coordinator.TaskFinished", &args, &reply)
	if !err {
		fmt.Println(err)
	}

}

func doMapTask(fileName string, NReduce int, taskId int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	intermediate := Mapf(fileName, string(content))
	buckets := make([][]KeyValue, NReduce, NReduce)
	for _, v := range intermediate {
		key := ihash(v.Key) % NReduce
		buckets[key] = append(buckets[key], v)
	}
	fileNames := map[int]string{}
	for i, v := range buckets {
		oname := "mr-" + string(i)
		fileNames[i] = oname
		ofile, _ := os.Create(oname)
		for _, vv := range v {
			fmt.Fprintf(ofile, "%v %v\n", vv.Key, vv.Value)
		}
	}
	callTaskFinished(MapTaskType, taskId, fileNames)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	os.Exit(0)
	return false
}
