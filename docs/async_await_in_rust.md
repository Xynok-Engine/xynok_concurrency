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
async fn foo1() -> usize
{
    println!("foo1: this is a future");
    10
}

fn foo2() -> impl std::future::Future<Output = usize>
{
    async {
        println!("foo2: this is also a future");
        20
    }
}
```
It's essentially just a transformation directive for the compiler. A piece of code is considered `async` if it creates a `std::future::Future`.
You can create your own async type, as long as it returns a future.
<details>
<summary>Example</summary>

```rust
struct MyAsync
{
    val: usize,
}
impl std::future::Future for MyAsync
{
    type Output = usize;

    // we'll ignore the content of this poll() for now and revisit it later
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output>
    {
        if self.val < 10
        {
            Poll::Ready(self.val)
        }
        else
        {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn main()
{

}
```
</details>

**How does it actually work?**

```rust 
#![allow(unused)]
async fn foo1() -> usize
{
    println!("foo1: this is a future");
    10
}

fn main()
{
    println!("Hello World");
    let x = foo1();
}
// Run this program, and the output will only be: "Hello World"
```

## References
- https://www.youtube.com/watch?v=ThjvMReOXYM
