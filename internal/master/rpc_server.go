package master

import (
	"log"
	"net"
	"net/rpc"
	"os"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

// rpcServerTarget is a dummy type that exposes only those methods that
// should be called via RPC.
type rpcServerTarget struct {
	master *Master
}

// RegisterWorker is called by workers after they have started up to
// report that they are ready to receive tasks.
func (rpcServerTarget rpcServerTarget) RegisterWorker(args *mr_rpc.RegisterArgs, _ *struct{}) error {
	util.Debug(
		"master @ %s received RegisterWorker RPC from worker @ %s\n",
		rpcServerTarget.master.Address,
		args.WorkerRPCAdress,
	)

	// The master's workerPoolManager is responsible for notifying folks
	// about this new worker.
	rpcServerTarget.master.workerPoolManager.SendNewWorker(args.WorkerRPCAdress)

	return nil
}

// ShutdownMaster is called to shut down the master.
func (rpcServerTarget rpcServerTarget) Shutdown(_, _ *struct{}) error {
	util.Debug(
		"master @ %s received Shutdown RPC\n",
		rpcServerTarget.master.Address,
	)

	rpcServerTarget.master.Shutdown()

	return nil
}

// masterRPCServer is responsible for listening for and executing RPC
// requests received from the network.
type masterRPCServer struct {
	// For logging, let's retain the address the RPC server runs on.
	address string
	// baseRPCServer does all the real work of handling RPC requests for
	// us.
	baseRPCServer *rpc.Server
	// connectionListener is used to accept connections from clients that
	// want to make RPC requests.
	connectionListener net.Listener
}

// startMasterRPCServer starts a masterRPCServer for the provided master.
func startMasterRPCServer(master *Master) *masterRPCServer {
	masterRPCServer := &masterRPCServer{}

	// Record the address to run the RPC server on.
	masterRPCServer.address = master.Address

	// Set up underlying RPC target and base server.
	rpcServerTarget := rpcServerTarget{master: master}
	masterRPCServer.baseRPCServer = rpc.NewServer()
	masterRPCServer.baseRPCServer.Register(rpcServerTarget)

	// Open socket where we will listen for incoming connections that will
	// make RPC requests.
	//
	// TODO: I think this os.Remove business is used in case a Unix socket
	// from a prior run is not cleaned up properly.
	os.Remove(masterRPCServer.address)
	connectionListener, err := net.Listen("unix", masterRPCServer.address)
	if err != nil {
		log.Fatalf(
			"master @ %v encountered error opening socket to receive RPCs: %v",
			master.Address,
			err,
		)
	}
	masterRPCServer.connectionListener = connectionListener

	// Run a background goroutine to listen for connecting users who want
	// to make RPCs.
	go masterRPCServer.listenForConnections()

	return masterRPCServer
}

// Shutdown shuts down the RPC server.
func (server *masterRPCServer) Shutdown() {
	// This will clean up the background goroutine because it will cause
	// connectionListener.Accept calls to fail.
	server.connectionListener.Close()
}

// Listens for connecting clients who want to make RPC calls.
func (server *masterRPCServer) listenForConnections() {
	for {
		// Keeps connecting clients who want to make RPCs. Note: when the
		// listener is closed on Shutdown, we will error out,
		connection, err := server.connectionListener.Accept()

		if err == nil {
			// Since RPC requests could be slow or blocking, do this in yet
			// another goroutine.
			go server.handleRPCConnection(connection)
		} else {
			// TODO: I really dislike this. How do we know that Accept failed
			// because of a clean Shutdown? There shouldn't be an error
			// message then.
			util.Debug(
				"master @ %v encountered RPC connection accept error: %v",
				server.address,
				err,
			)

			return
		}
	}
}

func (server *masterRPCServer) handleRPCConnection(connection net.Conn) {
	defer connection.Close()
	server.baseRPCServer.ServeConn(connection)
}
