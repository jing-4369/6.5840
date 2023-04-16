package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type AskWorkArgs struct {
	workerId int
}
type AskWorkReply struct {
	taskType  int // 1 for map task, 2 for reduce tas
	taskId    int
	fileName  string   // for map task
	fileNames []string // for reduce task
	nReduce   int
}

type TaskFinishedArgs struct {
	taskType  int
	taskId    int
	fileNames map[int]string
}
type TaskFinishedReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
