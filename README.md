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
- Queued writes: mutations go through queues and a single driver resolves them.
- Subscriber slots: a publish wakes only the components that read the signal, with no subscriber
set mutex, no reference count on read, and no value clone.
- Health tracking: reports how many replaced generations are still retained.
- External context integration: implement the write driver synchronization between dioxus and your
context for cross-VDOM interoperability.

## Read path

- A tracked read pins the generation, loads the value, and records the calling reactive context in
the signal's slot array.
- A cached read projects the pinned generation without copying the value.
- No `Arc` clone, no value clone, and no allocation happen on the read path.

## Subscriber passes

- The driver begins one pass per frame and publishes inside that pass.
- A slot is pruned after more than `PRUNE_AFTER_PASSES` passes without a read, so a mounted
component that stops reading stops rendering.
- A publish that lands right after a pass begins still marks a reader that read during the previous
pass, so an update is never dropped.

## Reclamation

- A replaced generation is dropped by a later retire, so it lags the replace by about two publish
intervals. Two generations stay retained, however long the signal runs.
- Flushing on the reading or the writing thread does not shorten that lag, because the release point
is a retire.
- A reader that stops reading keeps the generations retired since its last read, so the retained
count grows with the number of publishes missed.
- A dedicated receiver thread publishes an epoch and collects, so the writing thread never runs a
destructor.

## Health

- `Healthy` means every replaced generation was dropped.
- `Degraded` means a replaced generation is retained while publishes keep releasing older ones.
- `Stalled` means a whole window of publishes released nothing, which points at a reader that is
starving reclamation.

## License

This project is licensed under either the MIT license or the Apache-2 License.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in queued_signal by you, shall be licensed as MIT or Apache-2, without any additional terms or conditions.