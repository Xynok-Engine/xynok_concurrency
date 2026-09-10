---
title: Async & Await in Rust
excerpt: What are Async & Await? How do they work in Rust?
cover img: "../images/async_await.png"
tags:
  - data_structure
  - concurrency
---

## For Reader
After spending a full day researching, I have gathered a significant amount of documentation on implementing async properly in Rust. The scope of this work is massive, involving both complex code implementation and a deep dive into underlying system concepts. While I feel confident in building synchronization primitives, the primary hurdle with async, particularly regarding I/O tasks, is the necessity of interfacing directly with the Kernel.

Each operating system handles I/O communication differently. While libraries like `smol` or `mio` simplify this process, I want to understand exactly what is happening under the hood. Most of these existing libraries are primarily optimized for Linux, relying heavily on `epoll`.

As a game engine developer, my goal is cross-platform support. I need to target Linux, macOS, Windows, and potentially mobile or console platforms. To achieve a truly robust and cross-platform implementation, I need a deep understanding of how these libraries interact with their respective Kernels.

I have looked into Bevy, but it currently lacks support for a wide range of I/O protocols. It is limited to a few basic operations, whereas a production-ready game engine requires support for TCP and various other networking protocols. If I were to commit to building this out myself, I would likely spend several months just on research and implementation. 

Given the complexity and the current state of my understanding, I have decided to pause this work. I need to prioritize other critical features for the engine. 

- Game engines are predominantly CPU-bound rather than I/O-bound. It makes more sense to focus on CPU-intensive tasks first. 
- I am not abandoning the topic. I will continue to dedicate time each day to studying async patterns and Kernel internals so that I am better prepared to tackle this when the time is right.

## Overview
Nothing to see here yet, as I'm still diving into this topic.

## References
- [x] https://www.reddit.com/r/rust/comments/17f5qaa/anything_like_write_your_own_tokioasyncstd/
- [x] https://www.youtube.com/watch?v=ThjvMReOXYM: purely conceptual, doesn't even touch upon context, waker, or reactor
- [x] https://aibodh.com/posts/async-rust-chapter-1-hands-on-intro-to-async-rust/#future
- [x] https://www.youtube.com/watch?v=9_3krAQtD2k: this video at least provides a useful keyword regarding the kernel
- [x] https://ibraheem.ca/posts/too-many-web-servers/: a guide on transitioning from built-in Rust types to tokio for building a TCP server. It only covers futures and polling so far
- [ ] https://web.archive.org/web/20220505033253/https://cfsamson.github.io/books-futures-explained/: This is a gem. Although the author no longer maintains it officially, you can still find the source on the web archive.
    - [x] https://web.archive.org/web/20201123095507/https://github.com/cfsamson/books-futures-explained/issues/19: I was also very confused when I first looked into `Convar`, but his explanation gave me the confidence to work with it.
- [ ] https://medium.com/@souravdas08/building-a-minitokio-in-rust-how-does-tokio-work-24c916948e56
- [ ] https://medium.com/@bartekwinter3/build-an-async-runtime-in-rust-part-1-hooch-d43e3d96c3e6
- [ ] https://michaelhelvey.dev/posts/rust-async-runtime
- [ ] https://jacko.io/async_intro.html
- [ ] https://cubicy.icu/rust-async-demystified-p3/
- [ ] https://redixhumayun.github.io/async/2024/10/10/async-runtimes-part-iii.html
- [ ] https://rust-lang.github.io/async-book/
- [ ] https://github.com/mgattozzi/whorl/blob/main/src/lib.rs
- [ ] https://os.phil-opp.com/async-await/
- [ ] https://tokio.rs/tokio/tutorial/async
- [ ] https://doc.rust-lang.org/book/ch16-00-concurrency.html
- [ ] https://tuhuynh.com/posts/nio-under-the-hood/
- [ ] https://medium.com/@m-ibrahim.research/mastering-epoll-the-engine-behind-high-performance-linux-networking-85a15e6bde90
- [ ] https://man7.org/linux/man-pages/man7/epoll.7.html
- [ ] https://darkcoding.net/software/epoll-the-api-that-powers-the-modern-internet/
