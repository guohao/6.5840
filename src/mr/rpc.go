package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType string

const MAP TaskType = "map"
const REDUCE TaskType = "reduce"
const TRY_LATER TaskType = "try_later"
const NO_MORE TaskType = "no_more"

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
type AskTaskArgs struct {
}

type AskTaskReply struct {
	Id      int
	File    string
	NReduce int
	Type    TaskType
}
type CompleteTaskArgs struct {
	Id   int
	Type TaskType
}

type CompleteTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
