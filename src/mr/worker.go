package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	for {
		ok, askReply := askTask()
		if !ok || askReply.Type == TRY_LATER {
			time.Sleep(1 * time.Second)
			continue
		}
		if askReply.Type == NO_MORE {
			return
		}

		if askReply.Type == MAP {
			filename := askReply.File
			nReduce := askReply.NReduce
			id := askReply.Id
			intermediate := []KeyValue{}
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
			tmpFiles := make([]*os.File, nReduce)
			for i := 0; i < nReduce; i++ {
				tmpFiles[i], err = os.CreateTemp("", "mr-"+strconv.Itoa(id)+"-"+strconv.Itoa(i)+"tmp")
				if err != nil {
					log.Fatalf("cannot create temp file %v", err)
				}
			}
			for _, kv := range intermediate {
				err := json.NewEncoder(tmpFiles[ihash(kv.Key)%nReduce]).Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write to %v", tmpFiles[ihash(kv.Key)%nReduce].Name())
				}
			}
			for i := 0; i < nReduce; i++ {
				err = os.Rename(tmpFiles[i].Name(), "mr-"+strconv.Itoa(id)+"-"+strconv.Itoa(i))
				if err != nil {
					log.Fatalf("cannot rename %v", tmpFiles[i].Name())
				}
			}
			completeTask(id, MAP)
		}
		if askReply.Type == REDUCE {
			id := askReply.Id
			files, err := filepath.Glob("mr-*-" + strconv.Itoa(id))
			if err != nil {
				log.Fatal("Error:", err)
			}
			intermediates := []KeyValue{}
			for _, filename := range files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatal("Error:", err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediates = append(intermediates, kv)
				}
			}

			sort.Sort(ByKey(intermediates))

			i := 0
			ofile, _ := os.Create("mr-out-" + strconv.Itoa(id))

			for i < len(intermediates) {
				j := i + 1
				for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediates[k].Value)
				}
				output := reducef(intermediates[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediates[i].Key, output)

				i = j
			}
			completeTask(id, REDUCE)
		}
	}

}

func askTask() (bool, AskTaskReply) {
	args := AskTaskArgs{}
	reply := AskTaskReply{}
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		return ok, reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return false, reply
}

func completeTask(id int, taskType TaskType) {
	args := CompleteTaskArgs{}
	args.Id = id
	args.Type = taskType
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("reply.Y %v\n", reply.Y)
		return
	} else {
		fmt.Printf("call failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

	fmt.Println(err)
	return false
}
