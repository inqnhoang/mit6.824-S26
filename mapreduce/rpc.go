package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// example to show how to declare the arguments
// and reply for an RPC.
type GetTaskArgs struct{}

type GetTaskReply struct {
	Filename string
	TaskType string
	NReduce  int
	NMap     int
	BucketId int
}

type ReportTaskArgs struct {
	TaskType string
	Filename string
	BucketId int
}

type ReportTaskReply struct{}
