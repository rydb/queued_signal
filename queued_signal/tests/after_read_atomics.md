# Read and write costs after the atomic version and health words

The tracked read joins the subscriber slot array, and version and health loads use atomics.

- Profile: release
- Hot rows: 300 batch averages of 2000 operations
- Frame rows: 300 single reads after a 1 ms frame gap, with a publisher thread
- Threads: 1 reader, plus one publisher for the frame rows
- Units: ns is nanoseconds, us is microseconds, ms is milliseconds
- Store rows: a store reuses a published node when the pool has one and allocates when it
  does not, so the minimum and the median of a store row describe two different paths
- Publish rows: the version send happens only when a receiver waits, so a gate closed row
  and its one waiting receiver row differ by the cost of that send

## Read path

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| typed read, tracked | 4.33 ns | 7.59 ns | 8.10 ns | 4.32 ns | 8.10 ns |
| typed read, pin and load | 3.98 ns | 4.37 ns | 5.51 ns | 3.92 ns | 5.51 ns |
| typed deref | 0.25 ns | 0.51 ns | 0.66 ns | 0.24 ns | 0.66 ns |
| typed guard drop | 0.70 ns | 0.70 ns | 1.05 ns | 0.65 ns | 1.05 ns |
| read cached projection | 3.89 ns | 4.97 ns | 12.13 ns | 3.86 ns | 12.13 ns |
| peek version, atomic load | 1.20 ns | 1.72 ns | 6.42 ns | 1.20 ns | 6.42 ns |
| health, atomic load | 1.68 ns | 2.08 ns | 6.92 ns | 1.68 ns | 6.92 ns |
| tracked read with 1 held guards | 4.33 ns | 5.46 ns | 9.55 ns | 4.32 ns | 9.55 ns |
| tracked read with 8 held guards | 2.65 ns | 2.92 ns | 58.90 ns | 2.65 ns | 58.90 ns |
| tracked read with 64 held guards | 2.65 ns | 5.07 ns | 7.09 ns | 2.65 ns | 7.09 ns |

## Write path

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| publish, gate closed | 6.48 ns | 11.02 ns | 47.01 ns | 6.48 ns | 47.01 ns |
| publish value, gate closed | 36.62 ns | 62.23 ns | 94.94 ns | 36.46 ns | 94.94 ns |
| publish, one waiting receiver | 52.32 ns | 65.18 ns | 69.29 ns | 52.09 ns | 69.29 ns |

## Health scan

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| tick health scan with 1 registered | 6.17 ns | 9.76 ns | 10.61 ns | 6.15 ns | 10.61 ns |
| tick health scan with 8 registered | 6.17 ns | 8.11 ns | 9.19 ns | 6.15 ns | 9.19 ns |
| tick health scan with 64 registered | 6.17 ns | 9.72 ns | 9.86 ns | 6.16 ns | 9.86 ns |

## Erased read

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| erased read, owned guard | 3.99 ns | 4.72 ns | 7.31 ns | 3.94 ns | 7.31 ns |

## Value cell

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| atom read, pin and load | 3.92 ns | 4.89 ns | 5.87 ns | 3.84 ns | 5.87 ns |
| typed store | 19.53 ns | 24.21 ns | 24.91 ns | 19.30 ns | 24.91 ns |
| typed rcu | 20.69 ns | 28.22 ns | 29.14 ns | 20.40 ns | 29.14 ns |

## Frame cadence

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| frame read, cold | 90.00 ns | 251.00 ns | 271.00 ns | 50.00 ns | 271.00 ns |
| peek version, cold | 50.00 ns | 91.00 ns | 140.00 ns | 20.00 ns | 140.00 ns |
| pin and load, cold | 90.00 ns | 261.00 ns | 320.00 ns | 40.00 ns | 320.00 ns |
| tracked read, cold | 111.00 ns | 281.00 ns | 351.00 ns | 40.00 ns | 351.00 ns |
| frame read, 10 per frame | 8.10 ns | 17.00 ns | 55.20 ns | 5.00 ns | 55.20 ns |

## Summary

- The tracked read costs 4.33 ns and pin and load costs 3.98 ns, so the slot join adds 0.35 ns.
- A cached projection costs 3.89 ns, about 90 percent of the tracked read.
- Peeking the version costs 1.20 ns and reading health costs 1.68 ns, both one atomic load.
- A publish with no waiting receiver costs 6.48 ns, and the same publish with one waiting receiver costs 52.32 ns, so the version send costs 45.84 ns. A value publish with the gate closed costs 36.62 ns, and the typed store alone costs 19.53 ns.
- A cold frame read costs 90.00 ns, a cold pin and load 90.00 ns, and a cold version peek 50.00 ns.
- Ten reads of one signal in a frame cost 8.10 ns each with one pin.
