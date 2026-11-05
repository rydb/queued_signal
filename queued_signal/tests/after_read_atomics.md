# Read and write costs after the atomic version and health words

The tracked read joins the subscriber slot array, and version and health loads use atomics.

- Profile: release
- Hot rows: 300 batch averages of 2000 operations
- Frame rows: 300 single reads after a 1 ms frame gap, with a publisher thread
- Threads: 1 reader, plus one publisher for the frame rows
- Units: ns is nanoseconds, us is microseconds, ms is milliseconds

## Read path

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| typed read, tracked | 3.44 ns | 5.59 ns | 6.01 ns | 3.43 ns | 6.01 ns |
| typed read, pin and load | 1.49 ns | 1.49 ns | 1.92 ns | 1.48 ns | 1.92 ns |
| typed deref | 0.28 ns | 0.28 ns | 2.83 ns | 0.27 ns | 2.83 ns |
| typed guard drop | 0.76 ns | 0.85 ns | 1.60 ns | 0.70 ns | 1.60 ns |
| read cached projection | 1.62 ns | 1.65 ns | 4.69 ns | 1.60 ns | 4.69 ns |
| peek version, atomic load | 1.32 ns | 1.34 ns | 5.68 ns | 1.31 ns | 5.68 ns |
| health, atomic load | 1.58 ns | 1.60 ns | 4.09 ns | 1.58 ns | 4.09 ns |
| tracked read with 1 held guards | 3.44 ns | 3.52 ns | 6.13 ns | 3.43 ns | 6.13 ns |
| tracked read with 8 held guards | 3.18 ns | 5.31 ns | 5.92 ns | 3.17 ns | 5.92 ns |
| tracked read with 64 held guards | 3.18 ns | 3.59 ns | 5.49 ns | 3.17 ns | 5.49 ns |

## Write path

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| publish, watch notify only | 59.56 ns | 74.78 ns | 86.25 ns | 58.12 ns | 86.25 ns |
| publish value, store and notify | 108.58 ns | 171.31 ns | 178.91 ns | 98.95 ns | 178.91 ns |

## Health scan

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| tick health scan with 1 registered | 36.30 ns | 40.44 ns | 49.00 ns | 35.86 ns | 49.00 ns |
| tick health scan with 8 registered | 36.52 ns | 62.98 ns | 75.57 ns | 36.06 ns | 75.57 ns |
| tick health scan with 64 registered | 36.29 ns | 50.36 ns | 57.10 ns | 36.04 ns | 57.10 ns |

## Erased read

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| erased read, owned guard | 1.49 ns | 1.49 ns | 1.54 ns | 1.48 ns | 1.54 ns |

## Value cell

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| atom read, pin and load | 1.49 ns | 1.52 ns | 3.01 ns | 1.48 ns | 3.01 ns |
| typed store | 58.23 ns | 98.08 ns | 123.66 ns | 19.56 ns | 123.66 ns |
| typed rcu | 31.61 ns | 102.86 ns | 104.15 ns | 20.36 ns | 104.15 ns |

## Frame cadence

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| frame read, cold | 221.00 ns | 381.00 ns | 561.00 ns | 180.00 ns | 561.00 ns |
| peek version, cold | 120.00 ns | 141.00 ns | 161.00 ns | 100.00 ns | 161.00 ns |
| pin and load, cold | 220.00 ns | 400.00 ns | 480.00 ns | 190.00 ns | 480.00 ns |
| tracked read, cold | 281.00 ns | 441.00 ns | 521.00 ns | 210.00 ns | 521.00 ns |
| frame read, 10 per frame | 25.00 ns | 39.00 ns | 55.20 ns | 20.00 ns | 55.20 ns |

## Summary

- The tracked read costs 3.44 ns and pin and load costs 1.49 ns, so the slot join adds 1.95 ns.
- A cached projection costs 1.62 ns, about 47 percent of the tracked read.
- Peeking the version costs 1.32 ns and reading health costs 1.58 ns, both one atomic load.
- The typed store costs 58.23 ns, the watch notification 59.56 ns, and store plus notification 108.58 ns.
- A cold frame read costs 221.00 ns, a cold pin and load 220.00 ns, and a cold version peek 120.00 ns.
- Ten reads of one signal in a frame cost 25.00 ns each with one pin.
