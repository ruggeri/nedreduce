## TODO

**High**

* WorkerPool sendOffMessage method does an uncoordinated read of the
  runState??? WTF?
* Rework the README.

**MEDIUM**

* The WorkerPool won't immediately stop new work sets from starting when
  a Shutdown is submitted. If someone is submitting shutdown then
  presumably they don't want people to start more and more work sets...
* Could beautify the tests.
* Clean up the `util` merging code.
* Fix how we choose the final name of the output file. (also in `util`
  merging code).

**Low**

* JobCoordinator just rejects a submitted job if it is already working
  on one. It could be nice to wait and start the job when you can.
* Right now the WorkerPool just fails you, and then you can never
  re-enter. It could be cool if a worker could re-enter the pool.
* Right now if the user of the WorkerPool doesn't read the events,
  they'll block the pool. Is that a big deal?
* Get standalone tests so that they verify their output.
* `reducer.MergedInputIterator` might be faster with a heap. Meh.
* `reducer.sortReducerInputFile` would ideally do an external merge
  sort, but that is way too much effort.
