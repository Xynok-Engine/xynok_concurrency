---
title: Oneshot Channel
excerpt: One job hands exactly one value back to whoever is waiting for it.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/2/2a/Mailbox.svg/1280px-Mailbox.svg.png
tags:
  - concurrency
  - api
---
## Overview

This is how a job returns a result out of the pool, and it is deliberately a channel rather than the return value of a blocking call.

A caller holding a receiver rather than a value has never assumed "the call returned, so the result is here", so swapping what runs underneath touches nothing at the call site. Everything the [async lane](lanes.md) hands back is built out of this.

That promise has already been cashed in once. The receiver is a `Future`, so the same channel serves all three ways of waiting: park, run pool jobs, or await it from a [task](task.md) and hold no thread at all.

Sending consumes the sending end, so there is exactly one send, and receiving happens once as well.

## Data Structure

Both ends share four fields:

* **A ready flag**, released when the value is written and acquired when it is read, so seeing it set means seeing the contents too.
* **A dropped flag**, saying the sender left without sending anything.
* **The value slot**, uninitialised until the moment of the send.
* **A waiter**, written by whoever is waiting, just before they sleep or return `Pending`. It is either a thread handle to unpark or a waker to call, and the sending side does not care which.

The waiter is not captured when the channel is created, because the thread that creates a channel and the thread that waits on the result are not necessarily the same. A job can build a channel and hand the receiving end somewhere else entirely.

> [!IMPORTANT]
> **A dropped sender is a result too**
>
> A sender that leaves without sending has to say so, or the waiter waits for the rest of the process's life. This happens for real when a job panics halfway through: the pool catches the panic, the sender is dropped during the unwind, and the receiver gets nothing back instead of hanging forever.

## Operational Mechanism

### Sending
* Write the value, then publish the ready flag, then wake the waiter if one has signed up.
* Sending never blocks and never panics. The receiver may already be gone, and if so the value simply stays in the channel until the channel itself is dropped. The sender has nothing to worry about, which matters when the sender is a job running inside a pool.

### Receiving
* **Inside a pool**, waiting runs other pool jobs until the value arrives. The thread is still a participant, so idling wastes a core, and with nested joins the waiting thread might be the one that has to run the job it is waiting for.
* **Outside any pool**, waiting parks.
* **Polling** takes the value if it is there and hands the receiver back if it is not, so a caller that checks and moves on does not lose the channel.

### Awaiting
* Polling checks for a value, writes the waker down under the same lock a sleeping thread would use, then checks again. It is the parking dance below with a waker in place of a thread handle.
* The output is `Option<T>`, the same as a blocking receive: `None` means the sender left without sending, which happens for real whenever a job or a task panics halfway.
* Polling again after `Ready` gives `None`, since the value has already been taken. That is the ordinary `Future` contract; here it answers harmlessly instead of panicking.

### The parking dance
1. Check whether the value or the cancellation has already landed.
2. Take the waiter lock, check again, and write this thread's handle down.
3. Check once more, then park.

If the sender finished just before the handle went down, the second check sees it. If it finishes after, it reads the handle under the same lock and wakes the thread. There is no gap in between.

## References
- https://docs.rs/tokio/latest/tokio/sync/oneshot/index.html
- https://doc.rust-lang.org/std/thread/fn.park.html
