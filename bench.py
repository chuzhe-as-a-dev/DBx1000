#!/usr/bin/env python3
"""Benchmark driver for PER_OP hook performance evaluation.

Runs conflict-free TPC-C across multiple algorithms and thread counts.
Outputs CSV for analysis.

Usage:
  # Build and run full benchmark
  python3 bench.py --threads 1,2,4,8,16,24,48 --runs 3 -o bench_results.csv

  # Quick sanity check
  python3 bench.py --threads 1 --runs 1 --algs per_op_noop,no_wait

  # Custom build params
  python3 bench.py --max-txn 200000 --warmup 10000 --threads 1,2,4,8
"""

import argparse, csv, os, re, subprocess, sys

DEFAULT_ALGS = [
    'per_op_noop',   # zero CC overhead (upper bound)
    'per_op_2pl',    # hook-based NO_WAIT 2PL
    'per_op_occ',    # hook-based OCC
    'no_wait',       # baseline NO_WAIT 2PL
    'tictoc',        # baseline TicToc
    'silo',          # baseline Silo
]

CSV_FIELDS = [
    'algorithm', 'threads', 'run',
    'txn_cnt', 'abort_cnt', 'run_time', 'throughput',
    'time_man', 'time_index', 'time_cleanup', 'latency',
]

def parse_summary(output):
    m = re.search(r'\[summary\]\s*(.*)', output)
    if not m:
        return None
    fields = {}
    for pair in m.group(1).split(','):
        k, v = pair.strip().split('=')
        fields[k.strip()] = float(v.strip())
    return fields

def build(build_dir, max_txn, warmup):
    print(f'Building in {build_dir}/ (MAX_TXN_PER_PART={max_txn}, WARMUP={warmup})...')
    cmake_cmd = (
        f'cmake -S . -B {build_dir} -DCMAKE_BUILD_TYPE=RelWithDebInfo'
        f' -DMAX_TXN_PER_PART={max_txn} -DWARMUP={warmup}'
    )
    ret = os.system(f'{cmake_cmd} > /dev/null 2>&1')
    if ret != 0:
        print("ERROR: cmake configure failed", file=sys.stderr)
        sys.exit(1)
    ret = os.system(f'cmake --build {build_dir} --parallel > /dev/null 2>&1')
    if ret != 0:
        print("ERROR: build failed", file=sys.stderr)
        sys.exit(1)
    print('Build complete.')

def run_one(build_dir, alg, threads, extra_flags):
    binary = os.path.join(build_dir, f'rundb_{alg}_tpcc')
    if not os.path.exists(binary):
        return None
    cmd = f'{binary} -t{threads} -n{threads} {extra_flags}'
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    return parse_summary(result.stdout)

def main():
    parser = argparse.ArgumentParser(description='PER_OP benchmark driver')
    parser.add_argument('--threads', default='1,2,4,8,16,24,48',
                        help='Comma-separated thread counts')
    parser.add_argument('--runs', type=int, default=3, help='Runs per combination')
    parser.add_argument('--algs', default=None,
                        help='Comma-separated algorithms (default: all)')
    parser.add_argument('--tpcc-flags', default='-Tr0 -Ts0',
                        help='Extra TPCC flags (default: conflict-free)')
    parser.add_argument('--max-txn', type=int, default=100000,
                        help='MAX_TXN_PER_PART (default 100000)')
    parser.add_argument('--warmup', type=int, default=0,
                        help='WARMUP transactions (default 0)')
    parser.add_argument('--build-dir', default='build_perf')
    parser.add_argument('-o', '--output', default=None, help='Output CSV path')
    parser.add_argument('--skip-build', action='store_true', help='Skip rebuild')
    args = parser.parse_args()

    thread_counts = [int(x) for x in args.threads.split(',')]
    algs = args.algs.split(',') if args.algs else DEFAULT_ALGS

    if not args.skip_build:
        build(args.build_dir, args.max_txn, args.warmup)

    # Open CSV output
    out_file = open(args.output, 'w', newline='') if args.output else sys.stdout
    writer = csv.DictWriter(out_file, fieldnames=CSV_FIELDS)
    writer.writeheader()

    total = len(algs) * len(thread_counts) * args.runs
    done = 0

    for alg in algs:
        for threads in thread_counts:
            for run in range(args.runs):
                done += 1
                label = f'[{done}/{total}] {alg} t={threads} run={run+1}'
                print(f'{label}...', end=' ', flush=True, file=sys.stderr)

                summary = run_one(args.build_dir, alg, threads, args.tpcc_flags)
                if summary is None:
                    print('SKIP (missing binary or timeout)', file=sys.stderr)
                    continue

                tput = summary['txn_cnt'] / summary['run_time'] if summary['run_time'] > 0 else 0
                row = {
                    'algorithm': alg,
                    'threads': threads,
                    'run': run + 1,
                    'txn_cnt': int(summary['txn_cnt']),
                    'abort_cnt': int(summary['abort_cnt']),
                    'run_time': f'{summary["run_time"]:.6f}',
                    'throughput': f'{tput:.0f}',
                    'time_man': f'{summary["time_man"]:.6f}',
                    'time_index': f'{summary["time_index"]:.6f}',
                    'time_cleanup': f'{summary["time_cleanup"]:.6f}',
                    'latency': f'{summary["latency"]:.9f}',
                }
                writer.writerow(row)
                if args.output:
                    out_file.flush()
                print(f'txn={int(summary["txn_cnt"])} '
                      f'abort={int(summary["abort_cnt"])} '
                      f'tput={tput:.0f}', file=sys.stderr)

    if args.output:
        out_file.close()
        print(f'\nResults written to {args.output}', file=sys.stderr)

if __name__ == '__main__':
    main()
