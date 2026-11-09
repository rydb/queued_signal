# queued_signal

A Dioxus signal for shared, wait-free reads and queued writes across VDOMs and contexts.

## Overview

`QueuedSignal` publishes immutable generations of its value and keeps an old generation alive
until every reader that pinned it is done. A read pins the current generation and loads it, so it
never waits on a writer.

Writers enqueue mutations and one driver resolves them. The driver applies the queued mutations,
publishes a new generation, and wakes the readers that need it.

## Features

- Wait-free reads: a read pins a generation, loads it, and dereferences a pointer.
- Queued writes: each writer thread owns one unbounded queue, and a single driver resolves them.
- Health tracking: reports how many replaced generations are still retained.
- External context integration: implement the write driver synchronization between dioxus and your
context for cross-VDOM interoperability.

## License

This project is licensed under either the MIT license or the Apache-2 License.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in queued_signal by you, shall be licensed as MIT or Apache-2, without any additional terms or conditions.