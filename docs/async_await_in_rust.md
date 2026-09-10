---
title: Async & Await in Rust
excerpt: What are Async & Await? How do they work in Rust?
cover img: "../images/async_await.png"
tags:
  - data_structure
  - concurrency
---

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
