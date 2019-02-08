## TODO

**High**

* Review, refactor, and document the `mapreduce/master` and
  `mapreduce/worker` packages.

**Medium**

* `rpc.DoTaskArgs` is kinda gross, since it is reused for both map and
  reduce tasks.
  * Also, I feel like `Worker`s should be told the `MappingFunction` and
    `ReducingFunction` via the RPC. Else they are hard-coded at `Worker`
    initialization.

**Low**

* `reducer.MergedInputIterator` might be faster with a heap. Meh.
* `reducer.sortReducerInputFile` would ideally do an external merge
  sort, but that is way too much effort.
