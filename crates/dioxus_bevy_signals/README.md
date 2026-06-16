# dioxus_bevy_signals

Crate for mirroring bevy state to and from dioxus using QueuedSignals.

## Features

- Hooks for shared access to bevy resources, queries, and assets via synchronization mirrors.
- Automatic cleanup of unused hooks when dioxus components unmount.
- Configurable time-step for synchronization intervals.
