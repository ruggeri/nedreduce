## TODO

**High**

* One last review of code. Especially add documentation to workerpool.
* Rework the README.

**MEDIUM**

* Clean up the `util` merging code.
* Fix how we choose the final name of the output file. (also in `util`
  merging code).

**Low**

* Get standalone tests so that they verify their output.
* `reducer.MergedInputIterator` might be faster with a heap. Meh.
* `reducer.sortReducerInputFile` would ideally do an external merge
  sort, but that is way too much effort.
