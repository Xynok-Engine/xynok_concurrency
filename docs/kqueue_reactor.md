---
title: Kqueue Reactor
excerpt: How kqueue/kevent actually work, and what a hand-written async I/O reactor has to get right before it's safe.
cover img: "../images/kqueue_reactor.png"
tags:
  - concurrency
  - async
---

## Overview

This doc comes before any reactor code, on purpose. `kqueue`/`kevent` is a syscall pair with an `unsafe` FFI boundary: get a struct field width wrong, or free something the kernel still references, and the program doesn't crash, it just quietly corrupts a byte here and there. That's the kind of bug you want to have already reasoned about once, calmly, before writing the `extern "C"` block, not the kind you want to discover from a flaky test three weeks later.

Everything here is about the mechanism itself: what the syscalls promise, what each struct field means, and where the sharp edges are. The reactor code we write on top of this (`src/async_rt/reactor/kqueue.rs`) will follow this doc step by step, so if something here doesn't make sense, that's the place to stop and ask before we touch code.

Platform: this whole doc is Darwin/BSD kqueue. None of the struct layouts or constants below carry over to Linux `epoll`, which is a different API with a different ABI. If we ever add an epoll backend, it gets its own doc.

## What kqueue actually is

`kqueue()` creates a file descriptor that represents one event queue living in the kernel. Everything else happens through a second syscall, `kevent()`, which does two jobs in a single call:

1. Applies a list of changes (register, modify, or remove interest in some event).
2. Waits for and returns a list of events that are now ready.

```c
int kqueue(void);

int kevent(int kq, const struct kevent *changelist, int nchanges,
           struct kevent *eventlist, int nevents,
           const struct timespec *timeout);
```

Combining "describe what I care about" and "tell me what happened" into one call is the actual reason kqueue beats `select`/`poll`: those older APIs make you hand over your *entire* interest set on every single call. Here you only describe the delta, and the kernel remembers the rest. `changelist` and `eventlist` can even point at the same buffer.

`timeout` is `NULL` to block forever, or a `timespec` (zero-valued for "poll without blocking, return immediately").

An event is identified by the triple `(ident, filter, udata)` (the `udata` part only counts if you set `EV_UDATA_SPECIFIC`). Registering the same triple twice doesn't create a duplicate, it just updates the existing entry's flags. And this part matters for correctness, not just cleanup: the filter is re-run both when you register it (to catch a condition that's already true) and every time you try to collect events (to catch one that stopped being true). Readiness in kqueue is not a fact you get told once and can assume forever, it's re-checked at the point you take it.

## `struct kevent`, field by field (Darwin ABI)

Pulled straight from `man kqueue` and cross-checked against `sys/event.h` in the macOS SDK on this machine:

```c
struct kevent {
    uintptr_t ident;   /* what you're watching, often a fd */
    int16_t   filter;  /* which kernel filter, e.g. EVFILT_READ */
    uint16_t  flags;   /* what to do: EV_ADD, EV_ONESHOT, ... */
    uint32_t  fflags;  /* filter-specific flags, both in and out */
    intptr_t  data;    /* filter-specific payload */
    void      *udata;  /* opaque, kernel passes it through untouched */
};
```

What each field is for, and its Rust-side type:

- **ident** (`usize`): usually a raw fd, but not always, e.g. `EVFILT_TIMER` treats it as an arbitrary id you invent, `EVFILT_SIGNAL` treats it as a signal number.
- **filter** (`i16`, negative constants): `EVFILT_READ = -1`, `EVFILT_WRITE = -2`, `EVFILT_TIMER = -7`, `EVFILT_USER = -10`, and a handful more we won't use yet.
- **flags** (`u16`): the verbs, `EV_ADD` / `EV_DELETE` / `EV_ENABLE` / `EV_DISABLE` / `EV_ONESHOT` / `EV_CLEAR`, plus the output-only `EV_EOF` / `EV_ERROR`.
- **fflags** (`u32`): filter-specific detail, e.g. `NOTE_TRIGGER` to fire an `EVFILT_USER` event by hand.
- **data** (`isize`): filter-specific payload, e.g. bytes available to read, or the errno when `EV_ERROR` is set.
- **udata** (`*mut c_void`): the field we actually care about most. The kernel never looks inside it, just hands it back to you unchanged in the returned event. This is how we'll tag an event with "which task does this wake belong to."

The widths are the whole safety story here. `flags` is `u16`, not `u32`, unlike the field of the same name on some other platforms' event structs. Get that wrong (wrong width, wrong field order, wrong `#[repr]`) and every syscall still "succeeds", it just reads and writes the wrong bytes. This is why the Rust struct mirroring this one needs `#[repr(C)]` and each field typed exactly as above, no rounding to something that "feels equivalent" like `u32` for convenience.

## The filters our reactor needs

- **`EVFILT_READ`**: fires when a fd is readable. For a listening socket, "readable" means a connection is pending in the accept backlog. For a regular socket, `data` on return tells you how many bytes are available, and `EV_EOF` in `flags` means the peer closed their write side.
- **`EVFILT_WRITE`**: fires when a fd has room to write to. `data` returns free space in the send buffer.
- **`EVFILT_TIMER`**: an interval or one-shot deadline. `ident` here is not a fd, it's an id you pick yourself, and `data` (input) is the timeout in milliseconds by default (`fflags` can switch the unit). Repeats unless you also set `EV_ONESHOT`.
- **`EVFILT_USER`**: a synthetic event with no fd behind it at all, you trigger it yourself by re-registering with the `NOTE_TRIGGER` fflag set.

That last one solves a real problem: the reactor thread spends most of its life blocked inside `kevent()` with a `NULL` timeout. If some *other* thread spawns a new task, or needs the reactor to shut down, there is nothing to wake that blocked syscall, since it only reacts to filters it was told to watch. The classic portable fix is the "self-pipe trick" (register a pipe's read end via `EVFILT_READ`, write a byte to wake it). Since we're kqueue-only for now, `EVFILT_USER` does the same job without an extra fd: register it once at startup, and any thread can wake the reactor by re-arming it with `NOTE_TRIGGER`.

## Flags: what each one buys you

- **`EV_ADD`**: register the event (implies enabled, unless `EV_DISABLE` is also set). Registering the same `(ident, filter)` again just updates it in place, it does not duplicate.
- **`EV_DELETE`**: remove it. Not always required by hand, closing a fd auto-removes every kevent tied to it (this is explicit in the man page), but a non-fd-backed event like a timer id has no such cleanup, you own removing it.
- **`EV_ONESHOT`**: deliver the event once, then auto-remove it. Good fit for our poll loop, a task asks "wake me next time this fd is readable", gets told exactly once, and has to explicitly ask again if it's still not done, instead of the reactor nagging it on every loop iteration while the task is busy elsewhere.
- **`EV_CLEAR`**: reset the filter's internal state after delivering, instead of leaving it latched. `EVFILT_READ`/`EVFILT_WRITE` are level-triggered by default (they keep reporting "ready" every time you ask, as long as the condition holds); `EV_CLEAR` turns that into edge-triggered behavior (report once per transition). We want this, level-triggered readiness plus a task that isn't scheduled to run yet is a recipe for the same event flooding back on every poll of the kqueue.
- **`EV_EOF` / `EV_ERROR`**: never something you set, only something the kernel sets on the way back. Check `EV_EOF` for "peer closed" style conditions, check `EV_ERROR` (with the errno sitting in `data`) after a bulk `EV_ADD` made with `EV_RECEIPT`.

## Non-blocking I/O: the part kqueue does not do for you

kqueue's whole job is telling you "this fd is probably ready now." It does not make `read()`/`write()`/`accept()` non-blocking, that is a separate, unrelated fd property you set yourself with `fcntl(fd, F_SETFL, O_NONBLOCK)`. Two things follow from that:

1. Skip this step and the reactor is a nice-looking facade around a call that still blocks the calling thread completely, none of the wake machinery matters if the actual syscall parks the thread anyway.
2. "Ready" is a hint, not a guarantee, by the time you get around to calling `read()`, the condition can have changed. Always still be ready to get `EAGAIN`/`EWOULDBLOCK` back even after a readiness notification, and treat that as "re-register interest and wait again", not as a bug.

## Three lifetimes that all have to agree

1. **The fd itself.** Owned by whatever Rust wrapper holds it (an `AsyncTcpStream`, say). Drop it, it closes, the kernel auto-clears its kevents, per the man page. Fine on its own.
2. **The task/waker storage that `udata` points at.** Never move or reuse that slot while any live kevent still holds its old address in `udata`. If you free or reuse it too early, a leftover event delivered from the same `kevent()` batch dereferences a dangling pointer. This is the same "task complete" vs "every handle to it is now invalid" gap already called out in `examples/ASYNC_GUIDE.md`, just showing up again at the FFI boundary instead of inside the executor.
3. **The `Waker` you handed to someone else.** Whatever holds a clone of it (an `Arc`, typically) must stay alive until nothing can call `.wake()` on it anymore.

The rule that keeps all three in order: removal (`EV_DELETE`, or a guaranteed-processed close) must happen-before you free or repurpose whatever `udata` pointed at. Don't lean on "well, close() cleans it up eventually" timing when you're about to reuse that memory for something else right now.

## What "safe" means when the whole API is `unsafe`

- **Struct layout is on you.** The compiler cannot check that our Rust `#[repr(C)] struct Kevent` actually matches the kernel's expectation, we're the ones responsible for matching the man page and header exactly, for this specific OS.
- **errno discipline.** `kqueue()`/`kevent()` return `-1` and set `errno` on hard failure. Read it via `std::io::Error::last_os_error()` immediately after the call, before any other libc call has a chance to clobber it.
- **`EINTR` is not a real error.** A signal can interrupt a blocked `kevent()` call. That's a valid, expected return, not a fault, retry the call rather than surfacing it as an I/O error.

## References these claims are checked against

- `man kqueue` / `man kevent` on this machine, i.e. the actual Darwin ABI we're targeting, not a generic description.
- `/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/sys/event.h` for the exact numeric `EVFILT_*`/`EV_*`/`NOTE_*` constants.
- Reading (not copying) how `mio` and `polling` structure their kqueue backend and `udata` usage, purely as a second opinion on the FFI choices above before trusting them in code we write ourselves.

## Next

Phase 2 code follows this doc field for field: the `kqueue()`/`kevent()` `extern "C"` declarations, a `Kevent` struct with exactly the layout above, an `EV_SET`-equivalent constructor, and a small standalone example that registers `EVFILT_READ` on a real socket and watches the wake happen. We go through that piece by piece, not as one large file drop.
