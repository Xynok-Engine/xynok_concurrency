---
excerpt: A data structure that helps us minimize contention between threads.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/f/fd/Circular_Buffer_Animation.gif/500px-Circular_Buffer_Animation.gif?utm_source=en.wikipedia.org&utm_campaign=parser&utm_content=thumbnail
---
# Ring Buffer

## Overview

**The Thread Pool Challenge** 

When building a thread pool, the primary hurdle I encountered was managing concurrent access to a shared buffer. In the context of my ECS (Entity Component System) implementation, this buffer stores a large volume of jobs that the system needs to process.

Processing these jobs sequentially on a single thread is inefficient and creates a bottleneck. To improve performance, I need multiple threads to consume these jobs in parallel. However, this introduces a classic synchronization problem.

**The Concurrency Bottleneck**

The system architecture involves two distinct types of operations occurring simultaneously:
* **Producer:** The main thread pushes new jobs into the buffer as they are generated.
* **Consumers:** Multiple worker threads pull jobs from the buffer to execute them.

Because these threads operate in parallel, data contention is inevitable. If I rely on standard synchronization primitives like `Mutex` or `RwLock`, the overhead becomes significant. Frequent locking forces threads to wait, which increases latency and extends the time required to complete a single frame.

To maintain high performance, the goal is to minimize contention between threads as much as possible. A ring buffer is the ideal data structure for this scenario. By using a ring buffer, I can decouple the production and consumption of jobs, allowing for a more efficient, lock-free approach that keeps the engine running smoothly.

## Cấu trúc dữ liệu

## Flow chart 💀

```mermaid
---
config:
  layout: elk
---
flowchart TD
    Producer[Producer / Owner]
    Consumer[Consumer / Thief]

    Head["AtomicU64 Head<br/>pack steal, real"]
    Tail["Atomic Tail"]

    Steal["steal<br/>First Occupied / In-Flight"]
    Real["real<br/>First Unclaimed"]
    TailIdx["tail<br/>Next Write Position"]

    Ring["Ring Buffer Slots<br/>UnsafeCell MaybeUninit T"]

    Push{Push}
    FreeCheck{"Free space?<br/>capacity - tail + steal"}
    WriteSlot["Write T into slot<br/>slot = tail AND mask"]
    PublishTail["Publish tail + 1<br/>Release"]

    Pop{Pop}
    AvailableCheck{"Available?<br/>tail - real > 0"}
    PopCAS["CAS Head<br/>Claim one job"]
    ThiefActive{"steal != real?"}
    AdvanceBoth["Advance steal + real<br/>to real + 1"]
    AdvanceReal["Keep steal<br/>Advance real + 1"]
    ReadPop["Move T from claimed slot"]

    StealBatch{Steal Batch}
    LoadHead["Load Head<br/>Read steal + real"]
    BusyCheck{"steal != real?"}
    Busy["Steal Busy"]

    LoadTail["Load tail<br/>Acquire"]
    CalcBatch["n = min of available and max"]
    EmptyCheck{"n == 0?"}
    Empty["Steal Empty"]

    ClaimCAS["CAS Head<br/>Advance real by n"]
    ClaimedRegion["Claimed Region<br/>from old real to new real<br/>In Flight"]
    CopyBatch["Move claimed jobs<br/>to destination"]
    ReleaseCAS["CAS Head<br/>Set steal to current real<br/>Release"]
    Success["Steal Success"]

    Producer -->|Push| Push
    Push --> FreeCheck
    Steal -->|Read boundary| FreeCheck
    TailIdx -->|Read current tail| FreeCheck

    FreeCheck -->|No| PushFail["Return Err T"]
    FreeCheck -->|Yes| WriteSlot
    WriteSlot -->|Write before publish| Ring
    WriteSlot --> PublishTail
    PublishTail -->|Advance| TailIdx
    PublishTail -->|Store| Tail

    Producer -->|Pop| Pop
    Pop --> AvailableCheck
    Real -->|Read unclaimed boundary| AvailableCheck
    TailIdx -->|Read published boundary| AvailableCheck

    AvailableCheck -->|No| PopEmpty["Return None"]
    AvailableCheck -->|Yes| PopCAS
    PopCAS --> ThiefActive

    ThiefActive -->|No thief active| AdvanceBoth
    ThiefActive -->|Thief active| AdvanceReal

    AdvanceBoth -->|Atomic update| Head
    AdvanceBoth -->|Advance| Steal
    AdvanceBoth -->|Advance| Real

    AdvanceReal -->|Atomic update| Head
    AdvanceReal -->|Keep unchanged| Steal
    AdvanceReal -->|Advance| Real

    AdvanceBoth --> ReadPop
    AdvanceReal --> ReadPop
    Ring -->|Move claimed T| ReadPop
    ReadPop --> PopResult["Return Some T"]

    Consumer -->|Steal or steal batch| StealBatch
    StealBatch --> LoadHead
    Head -->|Load packed indices| LoadHead
    LoadHead --> BusyCheck

    BusyCheck -->|Yes| Busy
    BusyCheck -->|No| LoadTail
    Tail -->|Acquire| LoadTail
    LoadTail --> CalcBatch
    CalcBatch --> EmptyCheck

    EmptyCheck -->|Yes| Empty
    EmptyCheck -->|No| ClaimCAS

    ClaimCAS -->|CAS failed| LoadHead
    ClaimCAS -->|CAS succeeded| ClaimedRegion
    ClaimCAS -->|Update| Head
    ClaimCAS -->|Advance| Real

    ClaimedRegion -->|Protect from overwrite| CopyBatch
    Ring -->|Move claimed range| CopyBatch

    CopyBatch -->|After all jobs copied| ReleaseCAS
    ReleaseCAS -->|CAS failed, retry| ReleaseCAS
    ReleaseCAS -->|CAS succeeded| Head
    ReleaseCAS -->|Advance steal| Steal
    ReleaseCAS --> Success

    Steal -->|Occupied boundary| Occupied["Occupied<br/>tail - steal"]
    TailIdx --> Occupied

    Real -->|Available boundary| Available["Available<br/>tail - real"]
    TailIdx --> Available

    Invariant["Global Invariant<br/>steal less than or equal real<br/>real less than or equal tail<br/>All indices only move forward"]
    Steal -.-> Invariant
    Real -.-> Invariant
    TailIdx -.-> Invariant

    style Producer fill:#eef2ff,stroke:#818cf8
    style Consumer fill:#eef2ff,stroke:#818cf8

    style Push fill:#eef2ff,stroke:#818cf8
    style Pop fill:#eef2ff,stroke:#818cf8
    style StealBatch fill:#eef2ff,stroke:#818cf8
    style WriteSlot fill:#eef2ff,stroke:#818cf8
    style PublishTail fill:#eef2ff,stroke:#818cf8
    style PopCAS fill:#eef2ff,stroke:#818cf8
    style LoadHead fill:#eef2ff,stroke:#818cf8
    style LoadTail fill:#eef2ff,stroke:#818cf8
    style CalcBatch fill:#eef2ff,stroke:#818cf8
    style ClaimCAS fill:#eef2ff,stroke:#818cf8
    style CopyBatch fill:#eef2ff,stroke:#818cf8
    style ReleaseCAS fill:#eef2ff,stroke:#818cf8

    style Ring fill:#fff7ed,stroke:#fb923c
    style Head fill:#fff7ed,stroke:#fb923c
    style Tail fill:#fff7ed,stroke:#fb923c
    style Steal fill:#fff7ed,stroke:#fb923c
    style Real fill:#fff7ed,stroke:#fb923c
    style TailIdx fill:#fff7ed,stroke:#fb923c
    style ClaimedRegion fill:#fff7ed,stroke:#fb923c

    style PopResult fill:#f0fdf4,stroke:#4ade80
    style Success fill:#f0fdf4,stroke:#4ade80
    style Available fill:#f0fdf4,stroke:#4ade80
    style Occupied fill:#f0fdf4,stroke:#4ade80

    style PushFail fill:#f0fdfa,stroke:#2dd4bf
    style PopEmpty fill:#f0fdfa,stroke:#2dd4bf
    style Empty fill:#f0fdfa,stroke:#2dd4bf
    style Busy fill:#f0fdfa,stroke:#2dd4bf
    style Invariant fill:#f0fdfa,stroke:#2dd4bf
```

## References
- https://en.wikipedia.org/wiki/Circular_buffer
