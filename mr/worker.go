package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

// KeyValue is the type of the slice contents returned by the Map functions.
type KeyValue struct {
	Key   string
	Value string
}

// ihash(key) % NReduce is used to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is called by the MapReduce worker.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	duration := 2 * time.Second
	time.Sleep(duration)
	// Your worker implementation here.
	for {
		// Generate a random duration between 1 and 5 seconds
		minSleep := 1 // Minimum sleep time in seconds
		maxSleep := 3 // Maximum sleep time in seconds
		sleepDuration := time.Duration(rand.Intn(maxSleep-minSleep+1)+minSleep) * time.Second
		time.Sleep(sleepDuration)
		task := CallRequestTask()
		if task.Task == Mapper {
			file, err := os.Open(task.File)
			if err != nil {
				log.Fatalf("cannot open %v", task.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.File)
			}
			file.Close()
			nReduce := task.NumberOfReducer
			kva := mapf(task.File, string(content))
			intermediateFiles := make([][]KeyValue, nReduce)
			for i := 0; i < nReduce; i++ {
				intermediateFiles[i] = make([]KeyValue, 0)
			}
			for _, keyValue := range kva {
				hashKey := ihash(keyValue.Key)
				idx := hashKey % nReduce
				intermediateFiles[idx] = append(intermediateFiles[idx], keyValue)
			}

			for i, keyValueList := range intermediateFiles {
				fileName := fmt.Sprintf("intermediate_files/%d/kv_%d.json", i, task.TaskId)
				file, err := os.Create(fileName)
				if err != nil {
					fmt.Println("Error creating file:", err)
					return
				}
				defer file.Close() // Close the file when done
				encoder := json.NewEncoder(file)
				err = encoder.Encode(keyValueList)
				if err != nil {
					fmt.Println("Error encoding and writing to file:", err)
					return
				}

				fmt.Printf("Key-value list written to %s\n", fileName)
			}
		} else if task.Task == Reducer {
			currentDir, _ := os.Getwd()
			folderPath := currentDir + "/" + task.File
			kva := []KeyValue{}
			readFolderErr := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if !info.IsDir() {
					file, _ := os.Open(path)
					defer file.Close()
					dec := json.NewDecoder(file)
					for {
						var kv []KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv...)
					}
				}

				return nil
			})
			if readFolderErr != nil {
				fmt.Printf("Error walking the path: %v\n", readFolderErr)
			}
			kvaHashMap := make(map[string][]string)
			for _, keyValue := range kva {
				if _, exists := kvaHashMap[keyValue.Key]; !exists {
					// If the key doesn't exist, initialize it with an empty list
					kvaHashMap[keyValue.Key] = []string{}
				}
				kvaHashMap[keyValue.Key] = append(kvaHashMap[keyValue.Key], keyValue.Value)
			}
			var outputs bytes.Buffer
			for key, values := range kvaHashMap {
				output := reducef(key, values)
				outputs.WriteString(fmt.Sprintf("%v %v\n", key, output))
			}

			outputFilePath := fmt.Sprintf("mr-out-%d", task.ReducerIndex)

			// Create or open the file for writing (creates if it doesn't exist, truncates if it does)
			outputFile, err := os.Create(outputFilePath)
			if err != nil {
				fmt.Printf("Error creating file: %v\n", err)
				return
			}
			defer outputFile.Close() // Close the file when you're done with it
			// Write the string content to the file
			_, err = outputFile.WriteString(outputs.String())
			if err != nil {
				fmt.Printf("Error writing to file: %v\n", err)
				return
			}
		} else {
			os.Exit(1)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func CallRequestTask() TaskResponse {
	args := TaskRequest{}
	reply := TaskResponse{}
	callSuccess := call("Master.TaskHandler", &args, &reply)
	fmt.Println("Get reply: " + reply.String())
	if !callSuccess {
		os.Exit(1)
	}
	return reply
}

// CallExample is an example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// call sends an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()

	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
