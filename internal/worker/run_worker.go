package worker

import (
	"log"
	"net"
	"net/rpc"
	"os"

	"github.com/ruggeri/nedreduce/internal/util"
)

// RunWorker sets up a connection with the master, registers its address, and
// waits for tasks to be scheduled.
func RunWorker(jobCoordinatorAddress string, me string,
	nRPC int, parallelism *Parallelism,
) {
	util.Debug("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.nRPC = nRPC
	wk.parallelism = parallelism
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Panic("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	wk.register(jobCoordinatorAddress)

	// DON'T MODIFY CODE BELOW
	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}
	wk.l.Close()
	util.Debug("RunWorker %s exit\n", me)
}
