package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

	args := AskWorkArgs{}

	reply := AskWorkReply{}

	call("Coordinator.AskWork", &args, &reply)
	switch reply.TaskType {
	case MapTaskType: // map work
		doMapTask(reply.FileName, reply.NReduce, reply.TaskId)

	case ReduceTaskType: // reduce work
		doReduceWork(reply.FileNames, reply.TaskId)
	case 0:
		time.Sleep(time.Second)
	}
	callAskWork()

}
func callTaskFinished(taskType int, taskId int, fileNames map[int]string) {
	args := TaskFinishedArgs{}

	args.TaskType = taskType
	args.TaskId = taskId
	args.FileNames = fileNames
	reply := TaskFinishedReply{}

	call("Coordinator.TaskFinished", &args, &reply)

}

func doMapTask(fileName string, NReduce int, taskId int) {
	// fmt.Println("doMapTask: ", taskId)
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
		tempName := "tmp-mr-" + fmt.Sprint(taskId) + "-" + fmt.Sprint(i)
		tempf, _ := os.Create(tempName)
		fileNames[i] = tempName
		enc := json.NewEncoder(tempf)
		for _, vv := range v {
			enc.Encode(&vv)
		}
		tempf.Close()
	}
	callTaskFinished(MapTaskType, taskId, fileNames)
}

func doReduceWork(fileNames []string, taskId int) {
	// fmt.Println("doReduceWork: ", taskId)
	kva := []KeyValue{}
	for _, v := range fileNames {
		file, err := os.Open(v)
		if err != nil {
			log.Fatalf("cannot open %v", v)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	tempName := "mr-out-" + fmt.Sprint(taskId)
	tmpf, _ := os.Create(tempName)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := Reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpf, "%v %v\n", kva[i].Key, output)

		i = j
	}
	tmpf.Close()
	callTaskFinished(ReduceTaskType, taskId, map[int]string{0: tempName})

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		os.Exit(0)
	}
}
