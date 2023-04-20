package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	Idle       = 1
	InProgress = 2
	Completed  = 3
)
const (
	MapTaskType    = 1
	ReduceTaskType = 2
)

type Coordinator struct {
	// Your definitions here.
	midFiles           map[int][]string
	nReduce            int
	mapTasks           []mapTask
	reduceTasks        []reduceTask
	mapTaskFinished    int
	reduceTaskFinished int
}

type mapTask struct {
	task
	fileName string
}
type reduceTask struct {
	task
}

type task struct {
	state int // 1 for idle, 2 for in-progress, 3 for completed
	// workerId int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskWork(args *AskWorkArgs, reply *AskWorkReply) error {
	if c.mapTaskFinished == len(c.mapTasks) && c.reduceTaskFinished == len(c.reduceTasks) {
		// fmt.Println("Coordinator: all finish")
		return errors.New("Coordinator has exited")
	}
	if c.mapTaskFinished < len(c.mapTasks) {
		for i, v := range c.mapTasks {
			if v.state == Idle {
				v.state = InProgress
				// v.workerId = args.workerId
				reply.TaskType = MapTaskType
				reply.FileName = v.fileName
				reply.TaskId = i
				reply.NReduce = c.nReduce
				return nil
			}
		}
	}
	if c.mapTaskFinished < len(c.mapTasks) {
		return nil
	}
	for i, v := range c.reduceTasks {
		if v.state == Idle {
			v.state = InProgress
			// v.workerId = args.workerId
			reply.TaskType = ReduceTaskType
			reply.FileNames = c.midFiles[i]
			reply.TaskId = i
			return nil
		}
	}
	return nil
}

func (c *Coordinator) TaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	if args.TaskType == MapTaskType {
		fmt.Println("finish maptask ", args.TaskId)
		c.mapTaskFinished++
		c.mapTasks[args.TaskId].state = Completed
		for k, v := range args.FileNames {
			c.midFiles[k] = append(c.midFiles[k], v)
		}
		fmt.Printf("Coordinator: finish midFiles %v\n", c.midFiles)
	} else {
		fmt.Println("finish reducetask ", args.TaskId)
		c.reduceTaskFinished++
		c.reduceTasks[args.TaskId].state = Completed
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.mapTaskFinished == len(c.mapTasks) && c.reduceTaskFinished == len(c.reduceTasks) {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for _, file := range files {
		c.mapTasks = append(c.mapTasks, mapTask{task{Idle}, file})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, reduceTask{task{Idle}})
	}
	c.nReduce = nReduce
	c.midFiles = make(map[int][]string)
	c.server()
	return &c
}
