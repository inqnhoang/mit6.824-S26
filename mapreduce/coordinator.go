package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Idle = iota
	Running
	Finished
)

type Coordinator struct {
	nReduce int
	nMaps   int

	mapTasks    map[string]int
	mapTasksId  map[string]int
	reduceTasks map[int]int
	mu          sync.Mutex

	mapDone    int
	reduceDone int

	mapTime    map[string]time.Time
	reduceTime map[int]time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(_ *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMaps
	if !(c.mapDone == c.nMaps) {
		reply.TaskType = "MAP"

		for f := range c.mapTasks {
			if c.mapTasks[f] == Running && time.Since(c.mapTime[f]) > 10*time.Second {
				c.mapTasks[f] = Idle
			}

			if c.mapTasks[f] == Idle {
				reply.Filename = f
				reply.BucketId = c.mapTasksId[f]
				c.mapTasks[f] = Running
				c.mapTime[f] = time.Now()
				return nil
			}
		}
		reply.TaskType = "WAIT"

	} else if !(c.reduceDone == c.nReduce) {
		reply.TaskType = "REDUCE"
		reply.Filename = ""

		for r := range c.reduceTasks {
			if c.reduceTasks[r] == Running && time.Since(c.reduceTime[r]) > 10*time.Second {
				c.reduceTasks[r] = Idle
			}

			if c.reduceTasks[r] == Idle {
				reply.BucketId = r
				c.reduceTasks[r] = Running
				c.reduceTime[r] = time.Now()
				return nil
			}
		}
		reply.TaskType = "WAIT"
	} else {
		reply.TaskType = "DONE"
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, _ *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "MAP" {
		c.mapTasks[args.Filename] = Finished
		c.mapDone++

	} else {
		c.reduceTasks[args.BucketId] = Finished
		c.reduceDone++
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapDone == c.nMaps && c.reduceDone == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make(map[string]int),
		mapTasksId:  make(map[string]int),
		reduceTasks: make(map[int]int),
		mapTime:     make(map[string]time.Time),
		reduceTime:  make(map[int]time.Time),
	}

	for i, f := range files {
		c.mapTasks[f] = Idle
		c.mapTasksId[f] = i
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Idle
	}

	c.nReduce = nReduce
	c.nMaps = len(files)
	c.mapDone = 0
	c.reduceDone = 0

	c.server(sockname)
	return &c
}
