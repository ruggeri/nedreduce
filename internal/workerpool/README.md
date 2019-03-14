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

**Correction**. `workSetCompleted` handling **must** hold the mutex.
That's to avoid (1) a new caller to `BeginNewWorkSet` sees
`currentWorkSet != nil`, (2) the `workSetCompleted` handler sets
`currentWorkSet = nil` AND fires the condition variable, (3) the caller
to `BeginNewWorkSet` starts waiting for the condition variable. Luckily,
taking the mutex in `workSetCompleted` handling can't cause any deadlock
issues.

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
nil, then changes to `shuttingDown`. In `shuttingDown` mode, there
should be no more worker registrations sent to background thread.

`ShutdownWorker` will now wait until there are no more messages in
flight. This happens because neither `BeginNewWorkSet` nor
`RegisterWorker` send messages anymore. `ShutdownWorker` waits via a
`WaitGroup`: before a message send we increment the wait group, and when
a message is delivered we remove from the wait group.

We need to ask briefly about how to deal with multiple callers to
`Shutdown`. Both will be start to be woken when `workSetCompleted` is
handled and the `Cond` is fired. Only one will get the lock first and be
able to break out of the loop, though.

While waiting for the messages to clear out, we might as well drop the
lock. That means that *both* `Shtudown` invocations will break out of
the loop.

That's probably fine: both invocations are going to do the same thing:
set `runState` to `shutDown` and return. But we might as well acquire a
lock before changing the `runState` (just feels good). Only one of them
actually needs to set the `runState` to `shutDown`.

This gets us close to an original solution of mine. Yikes.

## Events

As discussed, there are three external methods:

1. BeginNewWorkSet
2. RegisterNewWorker
3. Shutdown

We might as well make `Shutdown` synchronous. `Shutdown` should not
return until the `WorkerPool` truly is shut down.

If someone wants to kick off shutdown but not wait, they can launch
their own goroutine. If they later decide they want to wait for shutdown
to be complete, they can call `Shutdown` on their main thread (since
repeated calls to `Shutdown` are safe).

Likewise, I don't think we need to do anything special for
`RegisterWorker`. It will technically block as it checks the `runState`,
but this is very briefly. It will then asynchronously send a message
(`registerWorkerMessage`) to the background thread.

Another reason that `Shutdown` and `RegisterWorker` need not return a
channel is because no interesting events occur in between method start
and method completion.

That is *not* the case for `BeginNewWorkSet`. We don't want to block in
this method, because it could take a while. However, if we don't block,
we won't know whether we could start the work set. So we want to push
down a message that says either `WorkerPoolCommencedWorkSet` or
`WorkerPoolDidNotAcceptWorkSet`.

There could be interesting notifications along the way. For instance, we
might want to pipe down progress as tasks complete. I won't do this, but
in theory it could be interesting to someone.

Of course, we want to know when the `WorkerPoolCompletedWorkSet`.

Sending events to the user over the channel could block the `WorkerPool`
if the user does not promptly read the events. To begin with, I will
rely on the user to consume the notifications promptly.

One thing I will *not* do is try to send the events asynchronously. That
is, I won't fire up a goroutine to push a notification into the channel
each time the `WorkerPool` wants to send one. Why not? **Because then
the notifications could be delivered out of order.**

A possibility is to use an "unbuffered channel." But I will investigate
that later.

## RaceCondition in assignTaskToWorker

First, I entirely forgot the message `assignTaskToWorker`. What the
fuck?

`assignTaskToWorker` needs to look at the `currentWorkSet`. Therefore,
this method *must* hold a lock, or we must only change `currentWorkSet`
in the background thread. I didn't do that, so I have a data race.

One way to fix is to not set `currentWorkSet` in the `BeginNewWork`
method, but to handle it in the `beginNewWork` message handler. If I do
that I won't have to worry about anyone changing `currentWorkSet`
anywhere in the background code. I don't have to worry about it in any
other message handlers than `assignTaskToWorker`, but the reason why was
kind of complicated (because all task completion/worker failure messages
must be handled before a work set can change).

If we do this, we'll have to check the `runState` inside the
`beginNewWorkSet` handler. That's because we can't start a new job if
we're shutting down the `WorkerPool`. We were going to have to lock
anyway, because `Shutdown` needs to look at `currentWorkSet`, so any
changes to that variable need to be coordinated.

Annoyingly, we'll still have to lock and check the `runState` in the
`BeginNewWorkSet` method. We still have to be aware of shut down before
trying to send messages to the background thread.

Luckily, I already use channels to communicate the result of
`BeginNewWorkSet`. And the solution of doing the update of
`currentWorkSet` in the background means I don't have to lock in
`assignTaskToWorker` every time I want to hand out a new task.

Okay, there's just one more problem here. What if two work sets are
submitted at around the same time? Both think they can start, so they
send messages to the background thread. But one of them can't, because
in the meantime the other has started.

`beginNewWorkSet` handling can't block on the background thread for the
already commenced work set to finish. But what we can do is move the
blocked `beginNewWorkSetMessage` back off the background thread. In a
new goroutine, we can wait until it may be possible to start the work
set, and try again.

Factoring out that common code is the point of
`tryToSendBeginNewWorkSetMessage`.
