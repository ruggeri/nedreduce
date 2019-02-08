This code comes from MIT 6.824. I have heavily adapted most of this
mapreduce code (refactoring it to a structure I like better), but all
the original work was done by someone else (rtm).

## TODO

* Fix DoTaskArgs to be less gross.
  * Also allow specification of the Mapping and Reducing functions.
  * The Workers presumably shouldn't have to know that in advance?
* Review master package more thoroughly, and Worker for the first time.

## Old Notes (from before refactor)

**common_map.go**

Has doMap function which performs mapping.

**common_reduce.go**

Has doReduce function which performs reducing.

**master_splitmerge.go**

Has a merge function that "concatenates" all parts by building a
complete map of key->value pairs, then writes these out in sorted order.

**master.go**

Master is the object that runs the master. It has a *Register* method
for workers to say they are registering (this just adds worker to a
list). Also has a method to *killWorkers* (collecting their statuses).

*Sequential* just runs every mapper sequentially, then every reducer.
It's 'schedule' function is trivial.

Let's look at *Distributed*. Distributed basically calls the *schedule*
method in `schedule.go`, but it also listens for workers to register,
and will forward the registration one-by-one to `schedule` via a
channel.

**schedule.go**

So let's check out `schedule.go`. It is empty and waiting for you to
write it! Basically, it will feed you workers that you can do RPC to.
The point of the RPC calls is to start Map and Reduce.

Presumably, at the end of all Reducers finishing work we call *Wait*?.

**master_rpc.go**

This has the RPC methods that you can call for the Master.

`startRPCServer` starts a server and registers the master. It starts
listening for people to connect. It will then register these folks with
the RPC server. (this keeps happening until the server is shutdown.)

`Shutdown` just stops connecting people and stops listening for people.

Somewhat weirdly, `stopRPCServer` tells the master to shutdown via RPC.
That prolly helps ensure that no one else is running an RPC call while
you shutdown.

Question: who shuts down the RPC server you started? I think no one
explicitly does; it is part of the finalizer?

`startRPCServer` starts a goroutine for each worker connecting, and then
just gives the worker connection over to the RPC server for
communication.

**common_rpc.go**

Just names some argument types.
