package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	FileList []string
	NReduce int
	mapJobCount int
	mapJobDoneCounter []bool
	mapJobTimer []time.Time
	reduceJobCount int
	reduceJobDoneCount []bool
	reduceJobTimer []time.Time
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// check if all the tasks are done
// should be in lock
func (c *Coordinator) checkTaskDone(isreduce bool) bool {
	if len(c.mapJobDoneCounter) < len(c.FileList) {
		return false
	}
	if isreduce {
		if len(c.reduceJobDoneCount) < c.NReduce {
			return false
		}
		for _, done := range c.reduceJobDoneCount {
			if !done {
				return false
			}
		}
		return true
	} else {
		for _, done := range c.mapJobDoneCounter {
			if !done {
				return false
			}
		}
		return true
	}
}

// check if some task got hang
// should be in lock
func  (c *Coordinator) checkTimeOutTask(isreduce bool) int {
	if isreduce {
		for i, done := range c.reduceJobDoneCount {
			if !done && time.Since(c.reduceJobTimer[i]) > 10 * time.Second {
				return i
			}
		}
		return -1
	} else {
		for i, done := range c.mapJobDoneCounter {
			if !done && time.Since(c.mapJobTimer[i]) > 10 * time.Second {
				return i
			}
		}
		return -1
	}
}

//
// custom RPC handler
// respond the map request from worker
func (c *Coordinator) AskMapTask(args *AskMapTaskArgs, reply *AskMapTaskReply) error {
	// Your code here.
	reply.NReduce = c.NReduce
	c.mu.Lock()
	if (c.mapJobCount == len(c.FileList)) {
		stuckJob := c.checkTimeOutTask(false)
		if stuckJob >= 0 {
			reply.TaskId = stuckJob
			reply.FileName = c.FileList[stuckJob]
			c.mapJobTimer[stuckJob] = time.Now()
			c.mu.Unlock()
			return nil
		} else {
			reply.TaskId = -1
			c.mu.Unlock()
			return nil
		}
	}
	reply.TaskId = c.mapJobCount
	reply.FileName = c.FileList[c.mapJobCount]
	c.mapJobCount++
	c.mapJobDoneCounter = append(c.mapJobDoneCounter, false)
	c.mapJobTimer = append(c.mapJobTimer, time.Now())
	c.mu.Unlock()
	return nil
}

// respond the reduce request from worker
func (c *Coordinator) AskReduceTask(args *AskReduceTaskArgs, reply *AskReduceTaskReply) error {
	// Your code here.
	c.mu.Lock()
	if (c.reduceJobCount == c.NReduce) {
		stuckJob := c.checkTimeOutTask(true)
		if stuckJob >= 0 {
			reply.TaskId = stuckJob
			reply.NMap = c.mapJobCount
			c.reduceJobTimer[stuckJob] = time.Now()
			c.mu.Unlock()
			return nil
		} else {
			reply.TaskId = -1
			c.mu.Unlock()
			return nil
		}
	}
	reply.TaskId = c.reduceJobCount
	if !c.checkTaskDone(false) {
		panic("map task not done\n")
	}
	reply.NMap = c.mapJobCount
	c.reduceJobCount++
	c.reduceJobDoneCount = append(c.reduceJobDoneCount, false)
	c.reduceJobTimer = append(c.reduceJobTimer, time.Now())
	c.mu.Unlock()
	return nil
}

// commit the task from worker
func (c *Coordinator) CommitTask(args *CommitTaskArgs, reply *CommitTaskArgsReply) error {
	// Your code here.
	if args.IsReduce {
		c.mu.Lock()
		if args.TaskId >= 0 {
			c.reduceJobDoneCount[args.TaskId] = true
			c.reduceJobTimer[args.TaskId] = time.Now()
			fmt.Println("reduce task done: ", args.TaskId)
		}
		reply.Result = c.checkTaskDone(true)
		c.mu.Unlock()
	} else {
		c.mu.Lock()
		if args.TaskId >= 0 {
			c.mapJobDoneCounter[args.TaskId] = true
			c.mapJobTimer[args.TaskId] = time.Now()
			fmt.Println("map task done: ", args.TaskId)
		}
		reply.Result = c.checkTaskDone(false)
		c.mu.Unlock()
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	ret = c.checkTaskDone(true) && c.checkTaskDone(false)
	c.mu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.FileList = append(c.FileList, files...)
	c.NReduce = nReduce
	c.mapJobCount = 0
	c.reduceJobCount = 0

	c.server()
	return &c
}
