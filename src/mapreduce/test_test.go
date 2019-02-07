package mapreduce

import (
	"fmt"
	"mapreduce/common"
	"mapreduce/mapper"
	"mapreduce/master"
	"mapreduce/reducer"
	"testing"
	"time"

	"bufio"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	nNumber = 100000
	nMap    = 20
	nReduce = 10
)

// Create input file with N numbers
// Check if we have N numbers in output file

// Split in words
func MappingFunction(inputFileName string, line string, mappingEmitterFunction mapper.MappingEmitterFunction) {
	common.Debug("Map %v\n", line)
	words := strings.Fields(line)
	for _, w := range words {
		kv := common.KeyValue{w, ""}
		mappingEmitterFunction(kv)
	}
}

func ReducingFunction(groupKey string, groupIteratorfunction reducer.GroupIteratorFunction, reducingEmitterFunction reducer.ReducingEmitterFunction) {
	// TODO: Why do they want this?
	kv := common.KeyValue{groupKey, ""}
	reducingEmitterFunction(kv)
}

// Checks input file agaist output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, files []string) {
	output, err := os.Open("mrtmp.test")
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		if err != nil {
			log.Fatal("check: ", err)
		}
		defer input.Close()
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i++
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 DoTask RPC.
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("A worker didn't do any work\n")
		}
	}
}

// Make input file
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			log.Fatal("mkInput: ", err)
		}
		w := bufio.NewWriter(file)
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *master.Master {
	files := makeInputs(nMap)
	masterPort := port("master")
	mr := master.Distributed("test", files, nReduce, masterPort)
	return mr
}

func cleanup(mr *master.Master) {
	mr.CleanupFiles()
	for _, f := range mr.Files {
		master.RemoveFile(f)
	}
}

func TestSequentialSingle(t *testing.T) {
	mr := master.Sequential("test", makeInputs(1), 1, MappingFunction, ReducingFunction)
	mr.Wait()
	check(t, mr.Files)
	checkWorker(t, mr.Stats)
	cleanup(mr)
}

func TestSequentialMany(t *testing.T) {
	mr := master.Sequential("test", makeInputs(5), 3, MappingFunction, ReducingFunction)
	mr.Wait()
	check(t, mr.Files)
	checkWorker(t, mr.Stats)
	cleanup(mr)
}

func TestParallelBasic(t *testing.T) {
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.Address, port("worker"+strconv.Itoa(i)),
			MappingFunction, ReducingFunction, -1, nil)
	}
	mr.Wait()
	check(t, mr.Files)
	checkWorker(t, mr.Stats)
	cleanup(mr)
}

func TestParallelCheck(t *testing.T) {
	mr := setup()
	parallelism := &Parallelism{}
	for i := 0; i < 2; i++ {
		go RunWorker(mr.Address, port("worker"+strconv.Itoa(i)),
			MappingFunction, ReducingFunction, -1, parallelism)
	}
	mr.Wait()
	check(t, mr.Files)
	checkWorker(t, mr.Stats)

	parallelism.mu.Lock()
	if parallelism.max < 2 {
		t.Fatalf("workers did not execute in parallel")
	}
	parallelism.mu.Unlock()

	cleanup(mr)
}

func TestOneFailure(t *testing.T) {
	mr := setup()
	// Start 2 workers that fail after 10 tasks
	go RunWorker(mr.Address, port("worker"+strconv.Itoa(0)),
		MappingFunction, ReducingFunction, 10, nil)
	go RunWorker(mr.Address, port("worker"+strconv.Itoa(1)),
		MappingFunction, ReducingFunction, -1, nil)
	mr.Wait()
	check(t, mr.Files)
	checkWorker(t, mr.Stats)
	cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.DoneChannel:
			check(t, mr.Files)
			cleanup(mr)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 tasks
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.Address, w, MappingFunction, ReducingFunction, 10, nil)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.Address, w, MappingFunction, ReducingFunction, 10, nil)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}
