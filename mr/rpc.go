package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

type TaskType string

const (
	Mapper  TaskType = "Mapper"
	Reducer TaskType = "Reducer"
	None    TaskType = "None"
)

type TaskRequest struct {
}

type TaskResponse struct {
	File            string
	Task            TaskType
	NumberOfReducer int
	TaskId          int
	ReducerIndex    int
}

func (t TaskResponse) String() string {
	return fmt.Sprintf("[%s, %s]", t.File, t.Task)
}

// ExampleArgs to show how to declare the arguments
// for an RPC.
type ExampleArgs struct {
	X int
}

// ExampleReply to show how to declare the reply
// for an RPC.
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
