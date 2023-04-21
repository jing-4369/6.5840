package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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
const TimeOut = 10

type Coordinator struct {
	// Your definitions here.
	midFiles           map[int][]string
	nReduce            int
	mapTasks           []mapTask
	reduceTasks        []reduceTask
	mapTaskFinished    int
	reduceTaskFinished int
	mutex              sync.Mutex
}

type mapTask struct {
	task
	fileName string
}
type reduceTask struct {
	task
}

type task struct {
	state     int // 1 for idle, 2 for in-progress, 3 for completed
	startTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskWork(args *AskWorkArgs, reply *AskWorkReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.mapTaskFinished == len(c.mapTasks) && c.reduceTaskFinished == len(c.reduceTasks) {
		// fmt.Println("Coordinator: all finish")
		return errors.New("Coordinator has exited")
	}
	// fmt.Println("mapTaskFinished: ", c.mapTaskFinished)
	// for i, v := range c.mapTasks {
	// 	fmt.Printf("mapTasks%v: %v, %v\n", i, v.state, v.startTime)
	// }
	if c.mapTaskFinished < len(c.mapTasks) {
		for i, v := range c.mapTasks {
			if v.state == Idle {
				// fmt.Println("Coordinator maptask: ", i)
				c.mapTasks[i].state = InProgress
				c.mapTasks[i].startTime = time.Now()
				reply.TaskType = MapTaskType
				reply.FileName = v.fileName
				reply.TaskId = i
				reply.NReduce = c.nReduce
				return nil
			}
		}
		for i, v := range c.mapTasks {
			if v.state == InProgress {
				if time.Now().Sub(v.startTime).Seconds() > TimeOut {
					// fmt.Println("reassign map: ", i)
					c.mapTasks[i].startTime = time.Now()
					reply.TaskType = MapTaskType
					reply.FileName = v.fileName
					reply.TaskId = i
					reply.NReduce = c.nReduce
					return nil
				}
			}
		}
	}
	if c.mapTaskFinished < len(c.mapTasks) {
		return nil
	}
	// fmt.Println("reduceTaskFinished: ", c.reduceTaskFinished)
	// for i, v := range c.reduceTasks {
	// 	fmt.Printf("reduceTasks%v: %v, %v\n", i, v.state, v.startTime)
	// }
	for i, v := range c.reduceTasks {
		if v.state == Idle {
			// fmt.Println("Coordinator reducetask: ", i)
			c.reduceTasks[i].state = InProgress
			c.reduceTasks[i].startTime = time.Now()
			reply.TaskType = ReduceTaskType
			reply.FileNames = c.midFiles[i]
			reply.TaskId = i
			return nil
		}
	}
	for i, v := range c.reduceTasks {
		if v.state == InProgress {
			if time.Now().Sub(v.startTime).Seconds() > TimeOut {
				// fmt.Println("reassign reduce: ", i)
				c.reduceTasks[i].startTime = time.Now()
				reply.TaskType = ReduceTaskType
				reply.FileNames = c.midFiles[i]
				reply.TaskId = i
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) TaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.mapTaskFinished == len(c.mapTasks) && c.reduceTaskFinished == len(c.reduceTasks) {
		// fmt.Println("Coordinator: all finish")
		return errors.New("Coordinator has exited")
	}
	if args.TaskType == MapTaskType {
		// fmt.Println("finish maptask ", args.TaskId)
		if c.mapTasks[args.TaskId].state == Completed {
			return nil
		}
		c.mapTaskFinished++
		c.mapTasks[args.TaskId].state = Completed
		for k, v := range args.FileNames {
			oname := "mr-" + fmt.Sprint(args.TaskId) + "-" + fmt.Sprint(k)
			os.Rename(v, oname)
			c.midFiles[k] = append(c.midFiles[k], oname)
		}
	} else {
		// fmt.Println("finish reducetask ", args.TaskId)
		if c.reduceTasks[args.TaskId].state == Completed {
			return nil
		}
		c.reduceTaskFinished++
		c.reduceTasks[args.TaskId].state = Completed
		oname := "mr-out-" + fmt.Sprint(args.TaskId)
		os.Rename(args.FileNames[0], oname)
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
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
		c.mapTasks = append(c.mapTasks, mapTask{task{Idle, time.Now()}, file})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, reduceTask{task{Idle, time.Now()}})
	}
	c.nReduce = nReduce
	c.midFiles = make(map[int][]string)
	c.mutex = sync.Mutex{}
	c.server()
	return &c
}
