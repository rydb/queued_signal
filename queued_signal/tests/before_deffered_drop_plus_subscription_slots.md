# Baseline read and write costs

Captured before deferred drops and subscription slots.

- Profile: release
- Samples: 300 batch averages of 2000 operations
- Threads: 1, uncontended
- Units: ns is nanoseconds, us is microseconds, ms is milliseconds

## Read path

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| typed read, tracked | 123.36 ns | 128.64 ns | 135.08 ns | 119.88 ns | 135.08 ns |
| typed read, pin and load | 1.29 ns | 2.31 ns | 3.63 ns | 1.29 ns | 3.63 ns |
| typed deref | 0.25 ns | 0.25 ns | 0.31 ns | 0.24 ns | 0.31 ns |
| typed guard drop | 0.75 ns | 0.76 ns | 1.05 ns | 0.70 ns | 1.05 ns |
| read cached projection | 2.05 ns | 2.09 ns | 5.12 ns | 2.05 ns | 5.12 ns |
| peek version, watch borrow | 4.25 ns | 4.26 ns | 6.71 ns | 4.20 ns | 6.71 ns |
| health, watch borrow | 4.34 ns | 4.75 ns | 8.26 ns | 4.32 ns | 8.26 ns |

## Reader registry

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| registry register and unregister | 54.50 ns | 59.48 ns | 62.10 ns | 54.01 ns | 62.10 ns |
| registry heartbeat | 38.17 ns | 41.23 ns | 41.37 ns | 36.63 ns | 41.37 ns |
| tracked read with 1 registered | 123.52 ns | 127.52 ns | 131.04 ns | 119.16 ns | 131.04 ns |
| tracked read with 8 registered | 124.92 ns | 128.79 ns | 130.58 ns | 121.29 ns | 130.58 ns |
| tracked read with 64 registered | 125.45 ns | 129.32 ns | 130.80 ns | 121.18 ns | 130.80 ns |

## Write path

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| publish, watch notify only | 55.88 ns | 58.99 ns | 59.79 ns | 55.19 ns | 59.79 ns |
| publish value, store and notify | 101.34 ns | 107.71 ns | 108.61 ns | 92.72 ns | 108.61 ns |

## Health scan

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| tick health scan with 1 registered | 69.34 ns | 73.03 ns | 78.53 ns | 67.45 ns | 78.53 ns |
| tick health scan with 8 registered | 76.23 ns | 80.88 ns | 86.76 ns | 74.41 ns | 86.76 ns |
| tick health scan with 64 registered | 119.92 ns | 124.64 ns | 128.54 ns | 118.44 ns | 128.54 ns |

## Erased read

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| erased read, owned guard | 1.29 ns | 1.30 ns | 3.53 ns | 1.29 ns | 3.53 ns |

## Value cell

| Operation | p50 | p99 | p999 | min | max |
|---|---:|---:|---:|---:|---:|
| atom read, pin and load | 1.29 ns | 1.33 ns | 3.33 ns | 1.29 ns | 3.33 ns |
| typed store | 88.54 ns | 149.84 ns | 185.70 ns | 81.39 ns | 185.70 ns |
| typed rcu | 87.99 ns | 135.93 ns | 163.07 ns | 76.12 ns | 163.07 ns |

## Summary

- The tracked read costs 123.36 ns, of which 122.07 ns is the registry and ticket.
- Pin and load alone costs 1.29 ns, so the registry is about 95 times the read work.
- Registration plus unregistration costs 54.50 ns, and one heartbeat costs 38.17 ns.
- A cached projection costs 2.05 ns, about 2 percent of the tracked read.
- The typed store costs 88.54 ns, the watch notification 55.88 ns, and store plus notification 101.34 ns.
