# queued_signal

A Dioxus signal for shared and blockless reads/queued writes across VDOM(s) and contexts.

## Overview

`QueuedSignal` allows simultaneous reads and writes via left-right double buffers. 
Readers get wait-free access to the latest snapshot of the signal's value.
Writers push mutations to qeueues and QueuedSignal resolves the winning mutation(s). 

## Features

- Non-wait reads: via left-write double buffer.
- Non-block queued writes: via queues instead of direct mutable access
- Health tracking: detects stalled readers and exposes a `HealthStatus` signal.
- External context integration: implement write driver synchronization between dioxus and your context
for cross-dom dioxus interoperability

## License

Apache 2.0 -- see [LICENSE](LICENSE).
