## Attribution

This code comes from MIT 6.824. I have heavily adapted most of this
nedreduce code (refactoring it to a structure I like better), but all
the original work was done by someone else (rtm).

## Code Organization and Overview

The general architecture is as follows:

* Core mapping and reducing functionality
  * Most of this code is contained in `nedreduce/internal/{types,
    mapper, reducer, util}`.
* Distributed coordination code
  * Most of this is contained in `nedreduce/internal/{rpc, master,
    worker}`.
* User facing code
  * Contained in `nedreduce/pkg`.

### nedreduce/internal/types

The `nedreduce/internal/types` package contains the core, common types
that will be used by other subpackages. The first is
`types.JobConfiguration`: this is how the user specifies the input
files, how many reducers to use, the mapping and reducing functions to
run.

The `types` package also contains typedefs for `MappingFunction` and
`ReducingFunction`. When the user runs a job, they must provide mapping
and reducing functions with the appropriate function type signatures. In
particular, `MappingFunction`s and `ReducingFunction`s both emit
`types.KeyValue`s.

### nedreduce/internal/mapper

The `nedreduce/internal/mapper` package contains all the code required
for performing mapping. Its most important method is `ExecuteMapping`.
To partition the output `types.KeyValue`s, a helper `OutputManager` was
written. The `OutputManager` takes care of opening output files, setting
up JSON encoders, and calculating which reducer should be sent each
output `types.KeyValue`.

I wrote a `mapper.Configuration` struct to contain the parameters for a
map task. It tracks `types.JobConfiguration` fairly closely, but also
contains a `MapTaskIdx`.

### nedreduce/internal/reducer

The `nedreduce/internal/reducer` package contains all the code required
for performing reducing. It is similarish to the
`nedreducer/internal/mapper` package. There is a `reducer.Configuration`
object, an `ExecuteReducing` function. There is a `reducer.InputManager`
that mirrors the `mapper.OutputManager` class.

There are some differences from the mapper code though.

First, there is code to sort the input files (`sortReducerInputFile`). I
do not love this code because it is one of the few places I load an
entire file's worth of data into memory. But it would be
super-super-overkill to write an external merge sort for this toy
nedreduce implementation.

Reducers need to iterate groups of `types.KeyValue`s which all have the
same key (for instance, all the reducer inputs for the word "the"). A
group's `types.KeyValue`s may live across the different input files (the
word "the" may be output by multiple reduce tasks). If all input files
were concatenated before sorting, then forming groups would be easy:
scan `types.KeyValue`s until you hit one with a new key (read "the" rows
until you hit a row for the word "tiny").

I don't want to merge the reducer input files into one big file which is
then sorted. I sort each reducer input file, and then I pull keys
one-by-one in ascending order using my `MergedInputIterator`. The
`MergedInputIterator` can do this by peeking only one row ahead in each
reducer input file.

Now that `MergedInputIterator` is giving a stream of `types.KeyValue` by
ascending `Key`, I wrote a `GroupingIterator` which iterates over groups
of `types.KeyValue`. I don't want to load the entire group into memory
at once, though. I want to pass the user's `ReducingFunction` an
iterator over each member of the group. So `GroupingIterator` returns a
series of `GroupIterator`s. The idea is that the user's reducer can
typically iterate the group without ever storing all the
`types.KeyValue`s of the group in memory at once.

### nedreduce/internal/util

The `nedreduce/internal/util` package contains utility code. Most of the
code is for file naming or debugging. There is also a utility to merge
all files output by a job.

### nedreduce/internal/rpc

The `nedreduce/internal/rpc` package contains argument and reply types
to be used when making or responding to RPC requests. There are three
kinds of RPC request:

* `Worker`s `Register` with the `Master`. This lets the `Master` know
  they can assign work to the `Worker`.
* A `Master` can tell a `Worker` to `DoTask`. A task is either a mapping
  task or a reducing task. The same `Worker` may be told to do multiple
  tasks.
* Both `Master`s and `Worker`s can be told to `Shutdown`.

Code in the `nedreduce/master` and `nedreduce/worker` packages use the
`Call` function defined in `nedreduce/rpc` to invoke these RPC calls.
This function is nothing more than a wrapper around Golang's `rpc.Dial`
and `rcp.Call` functions.

### nedreduce/internal/master


### nedreduce/internal/worker

**TODO(HIGH)**

### nedreduce/pkg

The `nedreduce/pkg` package contains code intended to be invoked by
user programs.

The important types of the `nedreduce/internal/types` are exported via
type aliases.

There are additionally several methods such as `RunSequentialJob`,
`RunDistributedJob`, and `RunWorker`. `nedreduce` users call these to
use the library.
