package nedreduce

// So that people can look at goroutines.
import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"

	"github.com/ruggeri/nedreduce/internal/job_coordinator"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
	"github.com/ruggeri/nedreduce/internal/worker"

	"bufio"
	"log"
	"os"
	"sort"
	"strconv"
)

const (
	nNumber = 100000
	nMap    = 20
	nReduce = 10
)

// Create input file with N numbers
// Check if we have N numbers in output file

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

func setup() (*types.JobConfiguration, *job_coordinator.JobCoordinator) {
	files := makeInputs(nMap)
	jobCoordinatorPort := port("jobCoordinator")

	jobConfiguration := types.NewJobConfiguration(
		"test",
		files,
		nReduce,
		"WordSplittingMappingFunctionForTest",
		"WordCountingReducingFunction",
		types.Distributed,
	)

	jobCoordinator := job_coordinator.StartJobCoordinator(jobCoordinatorPort)

	return jobConfiguration, jobCoordinator
}

func cleanup(jobConfiguration *types.JobConfiguration) {
	util.CleanupFiles(jobConfiguration)

	for _, f := range jobConfiguration.MapperInputFileNames {
		os.Remove(f)
	}
}

func TestSequentialSingle(t *testing.T) {
	util.SetPluginPath("../build/plugin.so")

	jobConfiguration := types.NewJobConfiguration(
		"testJobName",
		makeInputs(1),
		1,
		"WordSplittingMappingFunctionForTest",
		"WordCountingReducingFunction",
		types.Sequential,
	)

	defer cleanup(jobConfiguration)

	jobCoordinator := job_coordinator.StartJobCoordinator("coordinator")
	jobCoordinator.StartJob(jobConfiguration)
	jobCoordinator.WaitForJobCompletion("testJobName")

	check(t, jobConfiguration.MapperInputFileNames)
	// checkWorker(t, jobCoordinator.Stats)
}

func TestSequentialMany(t *testing.T) {
	util.SetPluginPath("../build/plugin.so")

	jobConfiguration := types.NewJobConfiguration(
		"testJobName",
		makeInputs(5),
		3,
		"WordSplittingMappingFunctionForTest",
		"WordCountingReducingFunction",
		types.Sequential,
	)

	defer cleanup(jobConfiguration)

	jobCoordinator := job_coordinator.StartJobCoordinator("coordinator")
	jobCoordinator.StartJob(jobConfiguration)
	jobCoordinator.WaitForJobCompletion("testJobName")

	check(t, jobConfiguration.MapperInputFileNames)
	// checkWorker(t, jobCoordinator.Stats)
}

func TestParallelBasic(t *testing.T) {
	util.SetPluginPath("../build/plugin.so")

	jobConfiguration, jobCoordinator := setup()
	for i := 0; i < 2; i++ {
		go worker.RunWorker(
			jobCoordinator.Address(),
			port("worker"+strconv.Itoa(i)),
			nil,
		)
	}

	defer cleanup(jobConfiguration)

	mr_rpc.SubmitJob(
		jobCoordinator.Address(), jobConfiguration,
	)
	mr_rpc.WaitForJobCompletion(
		jobCoordinator.Address(), jobConfiguration.JobName,
	)

	check(t, jobConfiguration.MapperInputFileNames)
	// checkWorker(t, jobCoordinator.Stats)
}

func TestParallelCheck(t *testing.T) {
	util.SetPluginPath("../build/plugin.so")

	jobConfiguration, jobCoordinator := setup()
	defer cleanup(jobConfiguration)

	parallelismTester := &worker.ParallelismTester{}
	for i := 0; i < 2; i++ {
		go worker.RunWorker(
			jobCoordinator.Address(),
			port("worker"+strconv.Itoa(i)),
			[]worker.EventListener{worker.EventListener(parallelismTester)},
		)
	}

	mr_rpc.SubmitJob(
		jobCoordinator.Address(), jobConfiguration,
	)
	mr_rpc.WaitForJobCompletion(
		jobCoordinator.Address(), jobConfiguration.JobName,
	)

	check(t, jobConfiguration.MapperInputFileNames)
	// checkWorker(t, jobCoordinator.Stats)

	if parallelismTester.MaxLevelOfParallelism() < 2 {
		t.Fatalf("workers did not execute in parallel")
	}
}

func TestOneFailure(t *testing.T) {
	util.SetPluginPath("../build/plugin.so")

	jobConfiguration, jobCoordinator := setup()
	defer cleanup(jobConfiguration)

	// Start 2 workers that fail after 10 tasks
	go worker.RunWorker(
		jobCoordinator.Address(),
		port("worker"+strconv.Itoa(0)),
		[]worker.EventListener{
			worker.EventListener(worker.NewRPCLimitKiller(10)),
		},
	)
	go worker.RunWorker(
		jobCoordinator.Address(),
		port("worker"+strconv.Itoa(1)),
		nil,
	)

	mr_rpc.SubmitJob(
		jobCoordinator.Address(), jobConfiguration,
	)
	mr_rpc.WaitForJobCompletion(
		jobCoordinator.Address(), jobConfiguration.JobName,
	)

	check(t, jobConfiguration.MapperInputFileNames)
	// checkWorker(t, jobCoordinator.Stats)
}

func TestManyFailures(t *testing.T) {
	util.SetPluginPath("../build/plugin.so")

	jobConfiguration, jobCoordinator := setup()
	defer cleanup(jobConfiguration)

	mr_rpc.SubmitJob(
		jobCoordinator.Address(), jobConfiguration,
	)

	doneChannel := make(chan struct{})
	go func() {
		mr_rpc.WaitForJobCompletion(
			jobCoordinator.Address(), jobConfiguration.JobName,
		)

		doneChannel <- struct{}{}
	}()

	i := 0
	done := false
	for !done {
		select {
		case <-doneChannel:
			check(t, jobConfiguration.MapperInputFileNames)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 tasks
			w := port("worker" + strconv.Itoa(i))
			go worker.RunWorker(
				jobCoordinator.Address(),
				w,
				[]worker.EventListener{
					worker.EventListener(worker.NewRPCLimitKiller(10)),
				},
			)
			i++
			w = port("worker" + strconv.Itoa(i))
			go worker.RunWorker(
				jobCoordinator.Address(),
				w,
				[]worker.EventListener{
					worker.EventListener(worker.NewRPCLimitKiller(10)),
				},
			)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}

func TestMain(m *testing.M) {
	// Fucking beautiful. Lets you dump the stacktraces of all goroutines
	// easily in the browser:
	//
	//   http://localhost:6060/debug/pprof/goroutine?debug=2
	//
	// Helped me find a deadlock real fast. Wowzo.
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	retCode := m.Run()
	os.Exit(retCode)
}
