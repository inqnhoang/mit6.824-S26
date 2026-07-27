package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}

		reportArgs := ReportTaskArgs{}
		reportReply := ReportTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			return
		}

		switch reply.TaskType {
		case "MAP":
			content, _ := os.ReadFile(reply.Filename)
			text := string(content)

			keyval := mapf(reply.Filename, text)

			files := make([]*os.File, reply.NReduce)
			encoders := make([]*json.Encoder, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				filename := fmt.Sprintf("mr-%d-%d", reply.BucketId, i)
				files[i], _ = os.Create(filename)
				encoders[i] = json.NewEncoder(files[i])
			}

			for _, kv := range keyval {
				bucket := ihash(kv.Key) % reply.NReduce
				encoders[bucket].Encode(kv)
			}

			for _, f := range files {
				f.Close()
			}

			reportArgs.TaskType = "MAP"
			reportArgs.Filename = reply.Filename
			call("Coordinator.ReportTask", &reportArgs, &reportReply)

		case "REDUCE":
			outfile := fmt.Sprintf("mr-out-%d", reply.BucketId)
			ofile, _ := os.Create(outfile)

			var intermediate []KeyValue
			for i := 0; i < reply.NMap; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, reply.BucketId)
				f, _ := os.Open(filename)
				decoder := json.NewDecoder(f)
				var keyval KeyValue

				for decoder.Decode(&keyval) == nil {
					intermediate = append(intermediate, keyval)
				}
				f.Close()
			}
			sort.Sort(ByKey(intermediate))

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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			reportArgs.TaskType = "REDUCE"
			reportArgs.BucketId = reply.BucketId
			call("Coordinator.ReportTask", &reportArgs, &reportReply)

			ofile.Close()
		case "WAIT":
			time.Sleep(time.Second)
		case "DONE":
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
