package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AskMapTaskArgs struct {
}

type AskMapTaskReply struct {
	TaskId int
	NReduce int
	FileName string
}

type AskReduceTaskArgs struct {
}

type AskReduceTaskReply struct {
	TaskId int
	NMap int
}

type CommitTaskArgs struct {
	IsReduce bool
	TaskId int
}

type CommitTaskArgsReply struct {
	Result bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
