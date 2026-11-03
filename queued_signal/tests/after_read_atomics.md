# Read and write costs after the atomic version and health words

The tracked read joins the subscriber slot array, and version and health loads use atomics.

- Profile: release
- Samples: 300 batch averages of 2000 operations
- Threads: 1, uncontended
- Units: ns is nanoseconds, us is microseconds, ms is milliseconds

## Read path

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| typed read, tracked | 3.16 ns | 5.56 ns | 11.51 ns | 3.13 ns | 11.51 ns |
| typed read, pin and load | 1.35 ns | 1.96 ns | 2.81 ns | 1.35 ns | 2.81 ns |
| typed deref | 0.25 ns | 0.25 ns | 0.28 ns | 0.24 ns | 0.28 ns |
| typed guard drop | 0.75 ns | 0.81 ns | 1.10 ns | 0.65 ns | 1.10 ns |
| read cached projection | 1.85 ns | 1.86 ns | 9.62 ns | 1.47 ns | 9.62 ns |
| peek version, atomic load | 1.20 ns | 1.56 ns | 1.68 ns | 1.20 ns | 1.68 ns |
| health, atomic load | 1.20 ns | 1.45 ns | 3.86 ns | 1.20 ns | 3.86 ns |
| tracked read with 1 held guards | 3.14 ns | 4.49 ns | 5.69 ns | 3.13 ns | 5.69 ns |
| tracked read with 8 held guards | 2.94 ns | 4.18 ns | 6.01 ns | 2.93 ns | 6.01 ns |
| tracked read with 64 held guards | 2.94 ns | 5.78 ns | 9.15 ns | 2.91 ns | 9.15 ns |

## Write path

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| publish, watch notify only | 54.89 ns | 58.55 ns | 60.14 ns | 53.18 ns | 60.14 ns |
| publish value, store and notify | 101.77 ns | 166.57 ns | 218.65 ns | 94.06 ns | 218.65 ns |

## Health scan

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| tick health scan with 1 registered | 32.61 ns | 41.06 ns | 51.48 ns | 32.54 ns | 51.48 ns |
| tick health scan with 8 registered | 32.65 ns | 35.46 ns | 36.33 ns | 32.54 ns | 36.33 ns |
| tick health scan with 64 registered | 32.64 ns | 35.56 ns | 36.06 ns | 32.54 ns | 36.06 ns |

## Erased read

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| erased read, owned guard | 1.35 ns | 2.28 ns | 2.74 ns | 1.35 ns | 2.74 ns |

## Value cell

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| atom read, pin and load | 1.35 ns | 1.36 ns | 1.40 ns | 1.35 ns | 1.40 ns |
| typed store | 25.44 ns | 87.17 ns | 89.65 ns | 17.68 ns | 89.65 ns |
| typed rcu | 29.70 ns | 75.57 ns | 75.93 ns | 18.48 ns | 75.93 ns |

## Summary

- The tracked read costs 3.16 ns and pin and load costs 1.35 ns, so the slot join adds 1.80 ns.
- A cached projection costs 1.85 ns, about 59 percent of the tracked read.
- Peeking the version costs 1.20 ns and reading health costs 1.20 ns, both one atomic load.
- The typed store costs 25.44 ns, the watch notification 54.89 ns, and store plus notification 101.77 ns.
