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
| typed read, tracked | 3.13 ns | 3.90 ns | 7.06 ns | 3.13 ns | 7.06 ns |
| typed read, pin and load | 1.39 ns | 1.76 ns | 5.04 ns | 1.35 ns | 5.04 ns |
| typed deref | 0.25 ns | 0.25 ns | 0.27 ns | 0.24 ns | 0.27 ns |
| typed guard drop | 0.70 ns | 0.76 ns | 1.05 ns | 0.60 ns | 1.05 ns |
| read cached projection | 1.47 ns | 1.50 ns | 9.59 ns | 1.45 ns | 9.59 ns |
| peek version, atomic load | 1.20 ns | 1.34 ns | 3.40 ns | 1.20 ns | 3.40 ns |
| health, atomic load | 1.44 ns | 1.87 ns | 8.43 ns | 1.44 ns | 8.43 ns |
| tracked read with 1 held guards | 3.13 ns | 3.14 ns | 8.02 ns | 3.13 ns | 8.02 ns |
| tracked read with 8 held guards | 2.89 ns | 3.68 ns | 7.25 ns | 2.88 ns | 7.25 ns |
| tracked read with 64 held guards | 2.89 ns | 4.05 ns | 5.33 ns | 2.88 ns | 5.33 ns |

## Write path

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| publish, gate closed | 6.25 ns | 8.10 ns | 12.99 ns | 6.04 ns | 12.99 ns |
| publish value, gate closed | 59.61 ns | 108.87 ns | 109.68 ns | 40.39 ns | 109.68 ns |
| publish, one waiting receiver | 52.48 ns | 55.77 ns | 71.17 ns | 52.17 ns | 71.17 ns |

## Health scan

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| tick health scan with 1 registered | 6.24 ns | 7.57 ns | 9.24 ns | 6.23 ns | 9.24 ns |
| tick health scan with 8 registered | 6.24 ns | 7.83 ns | 8.29 ns | 6.22 ns | 8.29 ns |
| tick health scan with 64 registered | 6.24 ns | 8.01 ns | 9.00 ns | 6.22 ns | 9.00 ns |

## Erased read

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| erased read, owned guard | 1.35 ns | 1.37 ns | 2.97 ns | 1.35 ns | 2.97 ns |

## Value cell

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| atom read, pin and load | 1.35 ns | 1.36 ns | 1.40 ns | 1.35 ns | 1.40 ns |
| typed store | 54.01 ns | 86.86 ns | 89.98 ns | 18.09 ns | 89.98 ns |
| typed rcu | 51.36 ns | 66.90 ns | 69.61 ns | 18.71 ns | 69.61 ns |

## Frame cadence

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| frame read, cold | 210.00 ns | 401.00 ns | 441.00 ns | 170.00 ns | 441.00 ns |
| peek version, cold | 120.00 ns | 191.00 ns | 230.00 ns | 90.00 ns | 230.00 ns |
| pin and load, cold | 200.00 ns | 391.00 ns | 411.00 ns | 170.00 ns | 411.00 ns |
| tracked read, cold | 100.00 ns | 291.00 ns | 341.00 ns | 70.00 ns | 341.00 ns |
| frame read, 10 per frame | 11.00 ns | 17.00 ns | 24.00 ns | 5.00 ns | 24.00 ns |

## Summary

- The tracked read costs 3.13 ns and pin and load costs 1.39 ns, so the slot join adds 1.74 ns.
- A cached projection costs 1.47 ns, about 47 percent of the tracked read.
- Peeking the version costs 1.20 ns and reading health costs 1.44 ns, both one atomic load.
- A publish with no waiting receiver costs 6.25 ns, and the same publish with one waiting receiver costs 52.48 ns, so the version send costs 46.24 ns. A value publish with the gate closed costs 59.61 ns, and the typed store alone costs 54.01 ns.
- A cold frame read costs 210.00 ns, a cold pin and load 200.00 ns, and a cold version peek 120.00 ns.
- Ten reads of one signal in a frame cost 11.00 ns each with one pin.
