## TODO

**High**

* Get parallel failure tests working by adding RPC error handling.
* Rework the README.

**MEDIUM**

* Clean up util merging code.
* Fix how we choose the final name of the output file.

**Low**

* Get standalone tests so that they verify their output.
* `reducer.MergedInputIterator` might be faster with a heap. Meh.
* `reducer.sortReducerInputFile` would ideally do an external merge
  sort, but that is way too much effort.
