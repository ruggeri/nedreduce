There are three WorkerPool methods called externally:

1. BeginNewWorkSet
2. RegisterNewWorker
3. Shutdown

Since a WorkerPool runs a background thread to process the job, and
because Shutdown must shut down this background thread, it is not safe
to blindly send messages to the background thread. There **must** be a
`runState` variable to record whether the WorkerPool is still accepting
messages, and there **must** be a condition variable so that threads can
wait for the `runState` to change.

There are five kinds of messages to the background thread:

* beginNewWorkSet
* taskCompleted
* workSetCompleted
* registerWorker
* workerFailed

Technically, `beginNewWorkSet` work can probably be done in the
`BeginNewWorkSet` method. But it is fine to do this work in the message
thread anyway. `beginNewWorkSet` does not need to change the `runState`
or `currentWorkSet`, as these are done by `BeginNewWorkSet` before the
message is sent.

The `taskCompleted` and `workerFailed` messages by necessity **must** be
processed before it is possible to fire `workSetCompleted`. Therefore,
these messages **do not** need to check the `runState` or whether there
is a `currentWorkSet`. There has to be.

The `workSetCompleted` message by necessity must change the
`currentWorkSet`. It does not need a lock to do this; no one else can
change the `currentWorkSet`. `workSetCompleted` does not need to change
the `runState`. It **must** broadcast to everyone that the
`currentWorkSet` has been reset to nil, though. Thus, the `Cond` must be
fired not just for `runState` changes; it must also fire for
`currentWorkSet` changes.

The last kind of message is `registerWorker`. Of course, like
`BeginNewWorkSet` and `Shutdown`, the `RegisterNewWorker` method must
hold the lock to make sure that it is safe to send this message to the
background thread.

A problem here is that `registerWorker` may be delivered at any time.
This brings up an interesting point. We know that `BeginNewWorkSet` has
to acquire a lock to set `currentWorkSet` before sending the
`beginNewWorkSet` method, but it may *drop* the lock before sending that
`beginNewWorkSet` message asynchronously. It is safe to do so because
`ShutdownWorker` cannot sneak in and close the channel, since
`ShutdownWorker` will wait until `currentWorkSet` is nil.

With `RegisterWorker`, how will we stop shutdown? One way is to hold the
lock through the message send. But that is dangerous! **No**! Since
handling of `workSetCompleted` needs the lock, there is the possibility
that `RegisterWorker` is blocked from sending the message, but handling
of `workSetCompleted` cannot finish because the lock cannot be acquired.

So the answer must be that `ShutdownWorker` needs to make sure there are
no registrations in flight before closing the background thread. The
simplest thing I can think of is have a `messagesInFlight` variable.
Thus, the `ShutdownWorker` must make sure there is no `currentWorkSet`,
and no `messagesInFlight`.

To maintain this variable, it makes sense to have a `sendOffMessage`
method, which takes the lock, increments the variable, and releases the
lock. In a similar fashion, the variable should be decremented by
`handleMessage`.

But now `ShutdownWorker` must wait for *two* conditions to hold. (1)
that `currentWorkSet` is nil, and (2) `messagesInFlight` is zero. One
way to accomplish this is to have `handleMessages` fire the `Cond`
whenever `messagesInFlight` hits zero. This causes spurious wakeups of
`BeginNewWorkSet` and `ShutdownWorker` when those are waiting for a
change to `currentWorkSet`. That can be handled though.

It also opens the possibility that (1) shutdown is submitted, (2) a job
ends, (3) registrations are in flight so shutdown can't happen yet, (4)
a new job is started.

Therefore, this suggests a `runState` of `shuttingDown`. In this mode,
no new jobs can begin. `ShutdownWorker` waits for `currentWorkSet` to be
nil, then changes to `shuttingDown`. It next waits for there to be no
more messages in flight. Is does this via a `WaitGroup`: before a
message send we increment the wait group, and when a message is
delivered we remove from the wait group.

This gets us close to an original solution of mine. Yikes.
