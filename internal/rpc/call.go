package rpc

import (
	"net/rpc"
)

// Call sends an RPC to the the server running at `rpcServerAddress` and
// invokes the `serviceMethod` method. It passes arguments `args`, waits
// for the reply, and places this in `reply`. The `reply` argument
// should be the address of an appropriate reply structure.
//
// Call returns true if the server responded, and false if Call received
// no reply from the server. `reply`'s contents are valid if and only if
// Call returned true.
//
// You should assume that Call will time out and return false after a
// while if it doesn't get a reply from the server.
//
// Please use Call to send all RPCs. Please don't change this function.
func Call(
	rpcServerAddress string,
	serviceMethod string,
	args interface{},
	reply interface{},
) bool {
	// TODO(MEDIUM): Make this more expressive about what kind of errors
	// can occur. For instance, we'll need to restart tasks if there are
	// network errors. But we also want to know about errors we can't
	// recover from.
	conn, err := rpc.Dial("unix", rpcServerAddress)
	if err != nil {
		return false
	}
	defer conn.Close()

	err = conn.Call(serviceMethod, args, reply)
	if err == nil {
		return true
	}

	return false
}
