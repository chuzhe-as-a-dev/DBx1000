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

Compile-time defaults are in `config.h`. The most important parameters:

| Parameter | Description |
|-----------|-------------|
| `CC_ALG` | Concurrency control algorithm (set per binary) |
| `WORKLOAD` | `YCSB` or `TPCC` (set per binary) |
| `THREAD_CNT` | Default thread count (overridden at runtime by `-t`) |
| `MAX_TXN_PER_PART` | Transactions per thread before simulation ends |
| `INDEX_STRUCT` | `IDX_HASH` (default) or `IDX_BTREE` |

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
