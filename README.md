<img src="logo/dbx1000.svg" alt="DBx1000 Logo" width="60%">

-----------------

DBx1000 is a single-node in-memory OLTP database benchmark for evaluating concurrency control (CC) algorithms at high thread counts. It implements 11 CC algorithms and two workloads (YCSB and TPC-C).

The original concurrency control scalability study is described in:

[1] Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker, [Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores](http://www.vldb.org/pvldb/vol8/p209-yu.pdf), VLDB 2014

Build
-----

Requires CMake 3.16+ and a C++11 compiler.

```bash
cmake -S . -B build
cmake --build build --parallel
```

The build produces one binary per (algorithm, workload) combination:

```
build/rundb_<alg>_<wl>
```

For example: `build/rundb_tictoc_ycsb`, `build/rundb_hekaton_tpcc`.

To change which algorithms or workloads are compiled, edit `DBX_ALGS` / `DBX_WORKLOADS` in `CMakeLists.txt`. Supported values:

- **CC algorithms**: `NO_WAIT`, `WAIT_DIE`, `DL_DETECT`, `TIMESTAMP`, `MVCC`, `HSTORE`, `OCC`, `TICTOC`, `SILO`, `VLL`, `HEKATON`
- **Workloads**: `YCSB`, `TPCC`, `TEST`

Test
----

```bash
python3 test.py
```

Run
---

```bash
./build/rundb_<alg>_<wl> [options]
```

For a full list of options:

```bash
./build/rundb_<alg>_<wl> -h
```

Key flags:

| Flag | Description |
|------|-------------|
| `-tINT` | Number of worker threads |
| `-rFLOAT` / `-wFLOAT` | Read / write fraction (YCSB) |
| `-zFLOAT` | Zipf theta (YCSB skew; 0 = uniform) |
| `-nINT` | Number of warehouses (TPC-C) |
| `-o FILE` | Output file for statistics |
| `--param=value` | Override a runtime string parameter |

Configuration
-------------

Compile-time defaults are in `config.h`. CMake `-D` flags override them via `#ifndef` guards at the top of `config.h`.

**Core**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CC_ALG` | `TICTOC` | Concurrency control algorithm (set per binary) |
| `WORKLOAD` | `YCSB` | Workload (set per binary) |
| `THREAD_CNT` | `4` | Default thread count (overridden at runtime by `-t`) |
| `PART_CNT` | `1` | Number of logical partitions |
| `PAGE_SIZE` | `4096` | Memory page size in bytes |
| `CL_SIZE` | `64` | Cache line size in bytes |
| `WARMUP` | `0` | Transactions to run before measurement starts |
| `MAX_TXN_PER_PART` | `100000` | Transactions per thread before simulation ends |
| `MAX_ROW_PER_TXN` | `64` | Max rows accessed per transaction |
| `ROLL_BACK` | `true` | Roll back row modifications on abort |
| `PRT_LAT_DISTR` | `false` | Print transaction latency distribution at end |

**Memory**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MEM_PAD` | `true` | Pad allocations to cache-line size to avoid false sharing |
| `MEM_ALIGN` | `8` | Minimum alignment of allocated blocks in bytes |
| `THREAD_ALLOC` | `false` | Use per-thread memory arena instead of global allocator |

**Index**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `INDEX_STRUCT` | `IDX_HASH` | Index structure: `IDX_HASH` or `IDX_BTREE` |
| `ENABLE_LATCH` | `false` | Enable latching in B-tree index |
| `CENTRAL_INDEX` | `false` | Use a single centralized index (vs. per-partition) |
| `BTREE_ORDER` | `16` | Fanout of each B-tree node |

**CC-algorithm-specific**

| Parameter | Algorithms | Default | Description |
|-----------|------------|---------|-------------|
| `TS_TWR` | TIMESTAMP | `false` | Enable Thomas Write Rule |
| `MAX_WRITE_SET` | OCC | `10` | Max write-set size per transaction |
| `HSTORE_LOCAL_TS` | HSTORE | `false` | Skip global timestamp for single-partition txns |

**YCSB**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SYNTH_TABLE_SIZE` | `10M` | Number of rows in the synthetic table |
| `ZIPF_THETA` | `0.6` | Zipfian skew (0 = uniform, →1 = highly skewed) |
| `READ_PERC` | `0.9` | Fraction of read operations |
| `WRITE_PERC` | `0.1` | Fraction of write operations |
| `SCAN_PERC` | `0` | Fraction of scan operations |
| `SCAN_LEN` | `20` | Rows read per scan |
| `REQ_PER_QUERY` | `16` | Row accesses per transaction |
| `PART_PER_TXN` | `1` | Logical partitions touched per transaction |
| `PERC_MULTI_PART` | `1` | Fraction of transactions that are multi-partition |
| `FIRST_PART_LOCAL` | `true` | Always access the local partition first |

**TPC-C**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `NUM_WH` | `1` | Number of warehouses |
| `DIST_PER_WARE` | `10` | Districts per warehouse |
| `PERC_PAYMENT` | `0.5` | Fraction of Payment transactions (remainder are New-Order) |
| `FIRSTNAME_LEN` | `16` | Max first-name field length |
| `LASTNAME_LEN` | `16` | Max last-name field length |

Outputs
-------

txn_cnt: The total number of committed transactions. This number is close to but smaller than THREAD_CNT * MAX_TXN_PER_PART. When any worker thread commits MAX_TXN_PER_PART transactions, all the other worker threads will be terminated. This way, we can measure the steady state throughput where all worker threads are busy.

abort_cnt: The total number of aborted transactions. A transaction may abort multiple times before committing. Therefore, abort_cnt can be greater than txn_cnt.

run_time: The aggregated transaction execution time (in seconds) across all threads. run_time is approximately the program execution time * THREAD_CNT. Therefore, the per-thread throughput is txn_cnt / run_time and the total throughput is txn_cnt / run_time * THREAD_CNT.

time_{wait, ts_alloc, man, index, cleanup, query}: Time spent on different components of DBx1000. All numbers are aggregated across all threads.

time_abort: The time spent on transaction executions that eventually aborted.

latency: Average latency of transactions.


Branches and Other Related Systems
----------------------------------

DBx1000 currently contains two branches: 

1. The master branch focuses on implementations of different concurrency control protocols described [1]. The master branch also contains the implementation of TicToc [2]

[2] Xiangyao Yu, Andrew Pavlo, Daniel Sanchez, Srinivas Devadas, [TicToc: Time Traveling Optimistic Concurrency Control](https://dl.acm.org/doi/abs/10.1145/2882903.2882935), SIGMOD 2016


2. The logging branch implements the Taurus logging protocol as described the [3]. The logging branch is a mirror of https://github.com/yuxiamit/DBx1000_logging.
    
[3] Yu Xia, Xiangyao Yu, Andrew Pavlo, Srinivas Devadas, [Taurus: Lightweight Parallel Logging for In-Memory Database Management Systems](http://vldb.org/pvldb/vol14/p189-xia.pdf), VLDB 2020

The following two distributed DBMS testbeds have been developed based on DBx1000

1. Deneva: https://github.com/mitdbg/deneva
2. Sundial: https://github.com/yxymit/Sundial    
