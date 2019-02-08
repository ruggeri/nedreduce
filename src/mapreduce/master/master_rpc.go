package master

import (
	"fmt"
	"log"
	"mapreduce/util"
	mr_rpc "mapreduce/rpc"
	"net"
	"net/rpc"
	"os"
)

// RegisterWorker is an RPC method that is called by workers after they
// have started up to report that they are ready to receive tasks.
func (mr *Master) RegisterWorker(args *mr_rpc.RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	util.Debug("RegisterWorker: worker %s\n", args.WorkerRPCAdress)
	mr.workers = append(mr.workers, args.WorkerRPCAdress)

	// tell forwardWorkerRegistrations() that there's a new workers[]
	// entry.
	mr.newWorkerConditionVariable.Broadcast()

	return nil
}

// Shutdown is an RPC method that shuts down the Master's RPC server.
func (mr *Master) Shutdown(_, _ *struct{}) error {
	util.Debug("Shutdown: registration server\n")
	// TODO: In theory shuts down the listener. But see my comment
	// below...
	close(mr.shutdown)
	// "causes the Accept to fail" -- see my comment below.
	mr.connectionListener.Close()

	return nil
}

// startRPCServer starts the Master's RPC server.
func (mr *Master) startRPCServer() {
	// Create an RPC Server for calling methods remotely on the master.
	rpcServer := rpc.NewServer()
	rpcServer.Register(mr)

	// Begin listening for incoming connections.
	//
	// TODO: I'm not sure why this os.Remove call is needed.
	os.Remove(mr.Address) // only needed for "unix"
	connectionListener, e := net.Listen("unix", mr.Address)
	if e != nil {
		log.Fatal("RegstrationServer ", mr.Address, " error: ", e)
	}
	mr.connectionListener = connectionListener

	// Now that we are listening on the master address, can fork off
	// accepting connections to another thread.
	go func() {
	loop:
		for {
			// Does a non-blocking poll to see if we should shutdown.
			select {
			case <-mr.shutdown:
				break loop
			default:
			}

			// Accept a next incoming connection for RPC.
			//
			// TODO: I think this will block, so the goroutine won't get
			// collected on shutdown? Or do we close the connectionListener
			// elsewhere which causes the Accept call to return an error? In
			// that case an unnecessary Debug will be printed.
			conn, err := mr.connectionListener.Accept()
			if err == nil {
				go func() {
					// Serve the connection's RPC requests in another thread.
					defer conn.Close()
					rpcServer.ServeConn(conn)
				}()
			} else {
				util.Debug("RegistrationServer: accept error: %v", err)
				break
			}
		}
		util.Debug("RegistrationServer: done\n")
	}()
}

// stopRPCServer stops the master RPC server. This must be done through
// an RPC to avoid race conditions between the RPC server thread and the
// current thread.
func (mr *Master) stopRPCServer() {
	var reply mr_rpc.ShutdownReply
	ok := mr_rpc.Call(mr.Address, "Master.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", mr.Address)
	}
	util.Debug("cleanupRegistration: done\n")
}
