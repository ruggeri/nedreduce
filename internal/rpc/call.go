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
	conn, err := rpc.Dial("unix", rpcServerAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Call(serviceMethod, args, reply)
}
