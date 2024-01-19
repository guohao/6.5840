package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState int

const IDLE TaskState = 0
const RUNNING TaskState = 1
const COMPLETED TaskState = 2

type Task struct {
	id    int
	state TaskState
}

type Coordinator struct {
	files       []string
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) checkTaskResult(task *Task) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.state == RUNNING {
		task.state = IDLE
	}
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Done() {
		reply.Type = NO_MORE
		return nil
	}
	for i := 0; i < len(c.files); i++ {
		task := &c.mapTasks[i]
		if task.state == IDLE {
			task.state = RUNNING
			reply.Id = i
			reply.Type = MAP
			reply.NReduce = c.nReduce
			reply.File = c.files[i]
			go c.checkTaskResult(task)
			return nil
		}
	}
	for i := 0; i < len(c.files); i++ {
		if c.mapTasks[i].state != COMPLETED {
			reply.Type = TRY_LATER
			return nil
		}
	}

	for i := 0; i < c.nReduce; i++ {
		task := &c.reduceTasks[i]
		if task.state == IDLE {
			task.state = RUNNING
			reply.Id = i
			reply.Type = REDUCE
			go c.checkTaskResult(task)
			return nil
		}
	}
	reply.Type = TRY_LATER
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Type == REDUCE {
		task := &c.reduceTasks[args.Id]
		task.state = COMPLETED
	} else if args.Type == MAP {
		task := &c.mapTasks[args.Id]
		task.state = COMPLETED
	} else {
		log.Fatal("unknown task type")
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
	if len(c.reduceTasks) == 0 {
		return false
	}
	for _, task := range c.reduceTasks {
		if task.state != COMPLETED {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce mapTasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := make([]Task, len(files))
	for i := range files {
		tasks[i] = Task{id: i, state: IDLE}
	}
	reduceTasks := make([]Task, nReduce)

	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = Task{id: i, state: IDLE}
	}

	c := Coordinator{files: files, mapTasks: tasks, nReduce: nReduce, reduceTasks: reduceTasks}
	c.server()
	return &c
}
