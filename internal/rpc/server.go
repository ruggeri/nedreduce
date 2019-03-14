package rpc

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"

	"github.com/ruggeri/nedreduce/internal/util"
)

// Server is responsible for listening for and executing RPC requests
// received from the network.
//
// This is really a convenience around Go's built-in `net/rpc` Server.
type Server struct {
	// For logging, let's retain the address the RPC server runs on.
	address string
	// baseRPCServer does all the real work of handling RPC requests for
	// us.
	baseRPCServer *rpc.Server
	// connectionListener is used to accept connections from clients that
	// want to make RPC requests.
	connectionListener net.Listener
}

// StartServer starts a Server for the provided target. Note that we are
// flexible on the target's name. That lets us use dummy classes with
// fewer RPC methods exposed.
func StartServer(
	address string,
	serverTargetName string,
	serverTarget interface{},
) *Server {
	server := &Server{}

	// Record the address to run the RPC server on.
	server.address = address

	// Set up underlying RPC target and base server.
	server.baseRPCServer = rpc.NewServer()
	server.baseRPCServer.RegisterName(serverTargetName, serverTarget)

	// Open socket where we will listen for incoming connections that will
	// make RPC requests.
	//
	// This os.Remove business is necessary in case the Unix socket from a
	// prior run was not cleaned up properly.
	os.Remove(server.address)
	connectionListener, err := net.Listen("unix", server.address)
	if err != nil {
		log.Panicf(
			"server running at %v encountered error opening socket to receive RPCs: %v",
			address,
			err,
		)
	}
	server.connectionListener = connectionListener

	// Run a background goroutine to listen for connecting users who want
	// to make RPCs.
	go server.listenForConnections()

	return server
}

// Shutdown shuts down the RPC Server.
func (server *Server) Shutdown() {
	// This will clean up the background goroutine running
	// listenForConnections because it will cause
	// connectionListener.Accept calls to fail.
	server.connectionListener.Close()
}

// Listens for connecting clients who want to make RPC calls.
func (server *Server) listenForConnections() {
	for {
		// Keeps connecting clients who want to make RPCs. Note: when the
		// listener is closed on Shutdown, we will error out,
		connection, err := server.connectionListener.Accept()

		if err == nil {
			// Since RPC requests could be slow or blocking, do this in yet
			// another goroutine. That way we can continue to connect new RPC
			// clients so that they can make more requests.
			go server.handleRPCConnection(connection)
		} else {
			// Gross. Closing the connectionListener in Shutdown gives the
			// listener goroutine an opaque error that we can't easily
			// interpret. What if the Accept error had some other (bad) cause?
			//
			// The situation is explained here:
			//   https://github.com/golang/go/issues/4373
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}

			// Else, something actually did go wrong!
			util.Debug(
				"server running at %v encountered RPC connection accept error: %v\n",
				server.address,
				err,
			)

			return
		}
	}
}

// handleRPCConnection simply hands off the net.Conn to the built-in
// `rpc.Server` so that it can do the real work.
func (server *Server) handleRPCConnection(connection net.Conn) {
	defer connection.Close()
	server.baseRPCServer.ServeConn(connection)
}
