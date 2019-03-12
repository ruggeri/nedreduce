package rpc

import (
	"net/rpc"
)

// Call invokes an RPC at the server. It synchronously waits for the
// reply. This is a simple wrapper around rpc.Dial and rpc.Client#Call.
func Call(
	rpcServerAddress string,
	serviceMethod string,
	args interface{},
	reply interface{},
) error {
	// TODO(MEDIUM): Make this more expressive about what kind of errors
	// can occur. For instance, we'll need to restart tasks if there are
	// network errors. But we also want to know about errors we can't
	// recover from.
	conn, err := rpc.Dial("unix", rpcServerAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Call(serviceMethod, args, reply)
}
