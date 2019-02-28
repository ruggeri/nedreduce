package mapreduce

import (
	"fmt"
	"io"
	"mapreduce/util"
	"mapreduce/master"
	. "mapreduce/types"
	"mapreduce/worker"
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
func wordSplittingMappingFunction(
	inputFileName string,
	line string,
	mappingEmitterFunction EmitterFunction,
) {
	// util.Debug("Map %v\n", line)
	words := strings.Fields(line)
	for _, w := range words {
		kv := types.KeyValue{Key: w, Value: ""}
		mappingEmitterFunction(kv)
	}
}

func wordCountingReducingFunction(
	groupKey string,
	groupIteratorfunction GroupIteratorFunction,
	reducingEmitterFunction EmitterFunction,
) {
	for {
		kv, err := groupIteratorfunction()

		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("unexpected reducing error: %v\n", err)
		}

		reducingEmitterFunction(*kv)
	}
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

	configuration := Newtypes.JobConfiguration(
		"test",
		files,
		nReduce,
		wordSplittingMappingFunction,
		wordCountingReducingFunction,
	)

	master := master.RunDistributedJob(&configuration, masterPort)
	return master
}

func cleanup(master *master.Master) {
	util.CleanupFiles(master.types.JobConfiguration)

	for _, f := range master.types.JobConfiguration.MapperInputFileNames {
		util.RemoveFile(f)
	}
}

func TestSequentialSingle(t *testing.T) {
	configuration := Newtypes.JobConfiguration(
		"test",
		makeInputs(1),
		1,
		wordSplittingMappingFunction,
		wordCountingReducingFunction,
	)

	master := master.RunSequentialJob(&configuration)
	master.Wait()
	check(t, master.types.JobConfiguration.MapperInputFileNames)
	checkWorker(t, master.Stats)
	cleanup(master)
}

func TestSequentialMany(t *testing.T) {
	configuration := Newtypes.JobConfiguration(
		"test",
		makeInputs(5),
		3,
		wordSplittingMappingFunction,
		wordCountingReducingFunction,
	)

	master := master.RunSequentialJob(&configuration)
	master.Wait()
	check(t, master.types.JobConfiguration.MapperInputFileNames)
	checkWorker(t, master.Stats)
	cleanup(master)
}

func TestParallelBasic(t *testing.T) {
	master := setup()
	for i := 0; i < 2; i++ {
		go worker.RunWorker(
			master.Address,
			port("worker"+strconv.Itoa(i)),
			wordSplittingMappingFunction,
			wordCountingReducingFunction,
			-1,
			nil,
		)
	}
	master.Wait()
	check(t, master.types.JobConfiguration.MapperInputFileNames)
	checkWorker(t, master.Stats)
	cleanup(master)
}

func TestParallelCheck(t *testing.T) {
	master := setup()
	parallelism := &worker.Parallelism{}
	for i := 0; i < 2; i++ {
		go worker.RunWorker(
			master.Address,
			port("worker"+strconv.Itoa(i)),
			wordSplittingMappingFunction,
			wordCountingReducingFunction,
			-1,
			parallelism,
		)
	}
	master.Wait()
	check(t, master.types.JobConfiguration.MapperInputFileNames)
	checkWorker(t, master.Stats)

	parallelism.Mu.Lock()
	if parallelism.Max < 2 {
		t.Fatalf("workers did not execute in parallel")
	}
	parallelism.Mu.Unlock()

	cleanup(master)
}

func TestOneFailure(t *testing.T) {
	master := setup()
	// Start 2 workers that fail after 10 tasks
	go worker.RunWorker(
		master.Address,
		port("worker"+strconv.Itoa(0)),
		wordSplittingMappingFunction,
		wordCountingReducingFunction,
		10,
		nil,
	)
	go worker.RunWorker(
		master.Address,
		port("worker"+strconv.Itoa(1)),
		wordSplittingMappingFunction,
		wordCountingReducingFunction,
		-1,
		nil,
	)
	master.Wait()
	check(t, master.types.JobConfiguration.MapperInputFileNames)
	checkWorker(t, master.Stats)
	cleanup(master)
}

func TestManyFailures(t *testing.T) {
	master := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-master.DoneChannel:
			check(t, master.types.JobConfiguration.MapperInputFileNames)
			cleanup(master)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 tasks
			w := port("worker" + strconv.Itoa(i))
			go worker.RunWorker(
				master.Address,
				w,
				wordSplittingMappingFunction,
				wordCountingReducingFunction,
				10,
				nil,
			)
			i++
			w = port("worker" + strconv.Itoa(i))
			go worker.RunWorker(
				master.Address,
				w,
				wordSplittingMappingFunction,
				wordCountingReducingFunction,
				10,
				nil,
			)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}
