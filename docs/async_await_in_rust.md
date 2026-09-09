---
title: Async & Await in Rust
excerpt: What are Async & Await? How do they work in Rust?
cover img: "../images/async_await.png"
tags:
  - data_structure
  - concurrency
---

## Overview

## What does a `Future` look like?

```rust
// this is a future
async fn foo1() {}

// this is also a future
fn foo2() -> impl std::future::Future<Output = ()> {}
```

It's just a sort of transformation directive to the compiler.
