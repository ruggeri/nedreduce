## TODO

**High**

* Rework the README.
* Refactor `mapreduce/worker` package.
* Get parallel failure tests working.
* `rpc.Server` doesn't try to figure out why `conn.Accept` fails.
* Clear out `killWorkers` code in master?
* Re-enable remote shutdown of Master.
* Rename Master to JobCoordinator.

**Medium**

* The `WorkerPoolManager` should, when a new worker is registered, check
  to see if it already has listed that worker as available.
* What is nRPC in `pkg/RunWorker`?
* Add error handling to RPC call?

**Low**

* Fix standalone tests so that they verify their output.
* `reducer.MergedInputIterator` might be faster with a heap. Meh.
* `reducer.sortReducerInputFile` would ideally do an external merge
  sort, but that is way too much effort.
