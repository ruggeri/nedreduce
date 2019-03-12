## TODO

**High**

* Get parallel failure tests working by adding RPC error handling.
* Rework the README.

**MEDIUM**

* Don't panic if worker receives a duplicate job or if a shut down
  worker receives a job.

**Low**

* Get standalone tests so that they verify their output.
* `reducer.MergedInputIterator` might be faster with a heap. Meh.
* `reducer.sortReducerInputFile` would ideally do an external merge
  sort, but that is way too much effort.
