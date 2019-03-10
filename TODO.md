## TODO

**High**

* Review, refactor, and document the `mapreduce/master` and
  `mapreduce/worker` packages.
* Get all of the `test-mr.sh` tests passing.
* Fix `test-wc.sh` test so that it verifies correct output.
* `rpc.Server` doesn't try to figure out why `conn.Accept` fails.
* Clear out `killWorkers` code in master?
* Rename Master to JobCoordinator.

**Medium**

* `rpc.DoTaskArgs` is kinda gross, since it is reused for both map and
  reduce tasks.
  * Also, I feel like `Worker`s should be told the `MappingFunction` and
    `ReducingFunction` via the RPC. Else they are hard-coded at `Worker`
    initialization.
* The `WorkerPoolManager` should, when a new worker is registered, check
  to see if it already has listed that worker as available.

**Low**

* `reducer.MergedInputIterator` might be faster with a heap. Meh.
* `reducer.sortReducerInputFile` would ideally do an external merge
  sort, but that is way too much effort.


internal/master/rpc_server.go:  // TODO: I think this os.Remove business is used in case a Unix socket
internal/master/rpc_server.go:                  // TODO: I really dislike this. How do we know that Accept failed
