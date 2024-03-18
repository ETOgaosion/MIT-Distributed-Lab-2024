package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapProcess(taskid int, nreduce int, filename string, mapf func(string, string) []KeyValue) {
	//  read file
	if taskid < 0 {
		// fmt.Println("taskid < 0, map jobs are already allocated")
		return
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// call map function
	kva := mapf(filename, string(content))
	// write to intermediate file
	intermediateKva := make([][]KeyValue, nreduce)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nreduce
		intermediateKva[reduceId] = append(intermediateKva[reduceId], kv)
	}
	for i := 0; i < nreduce; i++ {
		file, err = os.Create("mr-" + strconv.Itoa(taskid) + "-" + strconv.Itoa(i) + ".json")
		if err != nil {
			log.Fatalf("cannot create %v", "mr-map-"+strconv.Itoa(taskid))
		}
		enc := json.NewEncoder(file)
		sort.Sort(ByKey(intermediateKva[i]))
		for _, kv := range intermediateKva[i] {
			err = enc.Encode(&kv)
		}
		file.Close()
	}
}

func reduceProcess(taskid int, nmap int, reducef func(string, []string) string) {
	if taskid < 0 {
		// fmt.Println("taskid < 0, reduce jobs are already allocated")
		return
	}
	intermediate := []KeyValue{}
	for i := 0; i < nmap; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskid) + ".json"
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(taskid)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	taskid, nreduce, file := CallForMapTask()
	mapProcess(taskid, nreduce, file, mapf)
	for ; !CallForCommitTask(false, taskid); {
		taskid, nreduce, file = CallForMapTask()
		mapProcess(taskid, nreduce, file, mapf)
	}
	reducetaskid, nmap := CallForReduceTask()
	reduceProcess(reducetaskid, nmap, reducef)
	for ; !CallForCommitTask(true, reducetaskid); {
		reducetaskid, nmap = CallForReduceTask()
		reduceProcess(reducetaskid, nmap, reducef)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// custom RPC request
// send an RPC request to the coordinator to get a map task, wait for the response.
func CallForMapTask() (int, int, string) {
	args := AskMapTaskArgs{}
	reply := AskMapTaskReply{}
	ok := call("Coordinator.AskMapTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		os.Exit(-1)
	}
	return reply.TaskId, reply.NReduce, reply.FileName
}

// send an RPC request to the coordinator to get a reduce task, wait for the response.
func CallForReduceTask() (int, int) {
	args := AskReduceTaskArgs{}
	reply := AskReduceTaskReply{}
	ok := call("Coordinator.AskReduceTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		os.Exit(-1)
	}
	return reply.TaskId, reply.NMap
}

// send an RPC request to the coordinator to commit a task, wait for the response.
func CallForCommitTask(isreduce bool, taskid int) bool {
	args := CommitTaskArgs{}
	args.IsReduce = isreduce
	args.TaskId = taskid
	reply := CommitTaskArgsReply{}
	ok := call("Coordinator.CommitTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		os.Exit(-1)
	}
	return reply.Result
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
