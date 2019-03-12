## TODO

**High**

* Get parallel failure tests working by adding RPC error handling.
* `rpc.Server` doesn't try to figure out why `conn.Accept` fails.
* Rework the README.

**Low**

* Get standalone tests so that they verify their output.
* `reducer.MergedInputIterator` might be faster with a heap. Meh.
* `reducer.sortReducerInputFile` would ideally do an external merge
  sort, but that is way too much effort.
