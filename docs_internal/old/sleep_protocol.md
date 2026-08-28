---
title: Sleep Protocol
excerpt: How a worker goes to sleep without ever sleeping through a job that just arrived.
cover img: https://upload.wikimedia.org/wikipedia/commons/5/5e/Sleep_EEG_REM.png
tags:
  - concurrency
  - synchronization
---
## Overview

This is the easiest part of the pool to get wrong, so here is the wrong version first. A worker looks for work and finds none. A producer pushes a job and tries to wake someone, but nobody is asleep yet, so the call goes nowhere. The worker then goes to sleep, on the strength of what it saw a moment ago, and sleeps straight through the job it just missed.

Nothing crashes. The job sits in the queue and the worker never wakes, and the symptom is a frame that occasionally takes forever for no visible reason.

The fix has three pieces and all three are needed:

1. **One atomic word packing three numbers.** A producer bumps a counter in it; a worker reads that counter before it starts looking and may only sleep if it has not moved. A job that arrived while the worker was searching shows up as a changed counter.
2. **A list of parked threads**, so waking means waking exactly one, instead of shouting the whole pool awake to fight over one job.
3. **One last look by the final searcher** after it has written its name on the sleep list. This piece is not needed for correctness, it only avoids sleeping and being woken immediately.

> [!IMPORTANT]
> **Both sides must touch that word with a read-modify-write**
>
> The two sides read each other crosswise: a producer bumps the counter and then reads "is anyone asleep", while a worker writes its name down and then reads "did the counter move". On separate locations both can read stale values, which is the textbook store buffer shape. Packing them into one word is only half of it. Every RMW on one address lands in a single modification order, so whoever arrives second is guaranteed to see the first, while a plain load may return a stale value as long as it likes. The first version of this file used a load on the producer side, and the loom model with one worker hung within a few dozen interleavings.

## Data Structure

The word holds three numbers side by side:

* **An event counter** in the high 32 bits, one per new job. A worker reads it before searching and compares at sleep time.
* **A searching count** in the next 16 bits, how many workers are out hunting. As long as somebody is hunting, a producer does not need to wake anyone, because the hunter will trip over the job.
* **An awake count** in the low 16 bits. Equal to the worker count means nobody is asleep, so the sleeper list lock never has to be touched at all.

Sixteen bits holds 65 535 workers, four orders of magnitude past any real pool, and leaves the rest for the counter.

Alongside it sit the sleeper list, used as a stack so the most recently parked worker wakes first with the warmest cache, and one flag per worker distinguishing a real wakeup from a spurious one.

## Operational Mechanism

### Pushing a job
* Bump the counter and read the other two numbers in the same operation. This is the hot path, so both of the checks that follow exist to do nothing in the overwhelming majority of cases.
* If anyone is searching, return. A hunter will find the job, and waking someone else just gets two workers fighting over one job and one of them going back to sleep.
* If nobody is asleep, return. There is nobody to call.
* Otherwise wake exactly one, and count that worker as searching **right there** rather than when it actually wakes. Waiting would leave the searching count at zero in between, and every job pushed during that window would wake yet another worker, which is how you wake the whole pool for a handful of jobs.

### Searching
* Searching is capped at half the workers. It means CASing into other workers' rings, so a pool where everyone searches spends its cores kicking cache lines around instead of running jobs. Half is enough for new work to spread quickly.
* The cap has a floor of one, which is not rounding cosmetics. A pool of one or two workers taking a literal half gets a cap of zero, meaning nobody may ever steal, and work sitting in someone else's ring sits there forever. Exactly one hang put that floor there.
* A worker that stops searching because it found work checks whether it was the **last** hunter. If so, the place it took that job from may hold more and nobody is looking any more, so it wakes one more worker.

### Going to sleep
1. Take the sleeper list lock, mark this worker parked, and add it to the list.
2. Leave the awake ranks, and the searching ranks if it was searching, in one operation, so a producer never sees a half-updated state.
3. If the counter moved, or if this is the last searcher and one more look still finds work, unregister and skip the sleep entirely.
4. Otherwise park in a loop until the flag says this was a real wakeup, because `park` may return for no reason and a stale wakeup from earlier makes it return at once.

Unregistering can fail, and failure means someone already pulled this worker off the list, so a wakeup is on its way. In that case the worker simply parks and wakes right back up.

The whole sequence rests on the two cases having no gap between them. A job pushed **before** the worker leaves the ranks means the producer's operation landed first, so the worker reads a changed counter and does not sleep. A job pushed **after** means the worker's operation landed first, so the producer sees the decremented counts, knows someone just went to sleep, and wakes them. Two operations on one address always have an order.

## References
- https://docs.rs/loom/latest/loom/
- https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/scheduler/multi_thread/idle.rs
- https://github.com/golang/go/blob/master/src/runtime/proc.go
- https://doc.rust-lang.org/std/thread/fn.park.html
