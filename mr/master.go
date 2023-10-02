package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// Master for MapReduce
type Master struct {
	mapperFiles    []string
	mapperFilesIdx int
	count          int
	// channel for reducer files
	reducerFolders  []string
	reducerFilesIdx int
	nReduce         int
	mu              sync.Mutex
}

func (m *Master) TaskHandler(args *TaskRequest, reply *TaskResponse) error {
	m.mu.Lock()
	reply.NumberOfReducer = m.nReduce
	reply.TaskId = m.count
	if m.mapperFilesIdx < len(m.mapperFiles) {
		fmt.Println("Assign mapper task")
		reply.File = m.mapperFiles[m.mapperFilesIdx]
		m.mapperFilesIdx++
		reply.Task = Mapper
	} else if m.reducerFilesIdx < len(m.reducerFolders) {
		fmt.Println("Assign reducer task")
		reducerIdx := m.reducerFilesIdx
		reply.File = m.reducerFolders[reducerIdx]
		reply.ReducerIndex = reducerIdx
		m.reducerFilesIdx++
		reply.Task = Reducer
	} else {
		fmt.Println("No task to assign task")
		reply.File = ""
		reply.Task = None
		os.Exit(1)
	}
	m.count++
	m.mu.Unlock()
	return nil
}

// Example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// server starts a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done is called periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeMaster creates a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	len := len(files)
	m := Master{
		mapperFiles:     make([]string, 0, len),
		mapperFilesIdx:  0,
		reducerFolders:  make([]string, 0, len*3),
		reducerFilesIdx: 0,
		nReduce:         nReduce,
	}
	// Your code here.

	m.mu.Lock()
	for _, file := range files {
		m.mapperFiles = append(m.mapperFiles, file)
	}
	m.mu.Unlock()

	// pre-create folders to store intermediate files
	for i := 0; i < nReduce; i++ {
		path := fmt.Sprintf("intermediate_files/%d", i)
		os.MkdirAll(path, os.ModePerm)
		m.reducerFolders = append(m.reducerFolders, path)
	}

	fmt.Println("server start")
	m.server()
	return &m
}
