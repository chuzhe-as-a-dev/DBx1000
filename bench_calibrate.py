#!/usr/bin/env python3
"""Calibrate benchmark duration and warmup for stable throughput measurements.

Builds binaries with varying MAX_TXN_PER_PART and WARMUP values, runs each
configuration multiple times, and reports throughput mean/CV per setting.

Usage:
  python3 bench_calibrate.py --alg no_wait --threads 8 --runs 5
  python3 bench_calibrate.py --alg no_wait --threads 8 --runs 5 --warmup 10000
  python3 bench_calibrate.py --alg per_op_2pl --threads 8 --runs 5 \
      --txn-values 10000,50000,100000,200000,500000
"""

import argparse, os, re, subprocess, sys, math

def parse_output(output):
    """Parse SimTime from PASS! line and fields from [summary] line."""
    sim_m = re.search(r'PASS!\s*SimTime\s*=\s*(\d+)', output)
    if not sim_m:
        return None
    sum_m = re.search(r'\[summary\]\s*(.*)', output)
    if not sum_m:
        return None
    fields = {}
    for pair in sum_m.group(1).split(','):
        k, v = pair.strip().split('=')
        fields[k.strip()] = float(v.strip())
    fields['sim_time'] = int(sim_m.group(1)) / 1e9  # wall-clock seconds
    return fields

def build(build_dir, max_txn, warmup):
    """Build with specific MAX_TXN_PER_PART and WARMUP."""
    cmake_cmd = (
        f'cmake -S . -B {build_dir} -DCMAKE_BUILD_TYPE=RelWithDebInfo'
        f' -DMAX_TXN_PER_PART={max_txn} -DWARMUP={warmup}'
        f' > /dev/null 2>&1'
    )
    ret = os.system(cmake_cmd)
    if ret != 0:
        print(f"ERROR: cmake configure failed", file=sys.stderr)
        sys.exit(1)
    ret = os.system(f'cmake --build {build_dir} --parallel > /dev/null 2>&1')
    if ret != 0:
        print(f"ERROR: build failed", file=sys.stderr)
        sys.exit(1)

def run_one(build_dir, alg, threads, extra_flags=''):
    """Run a single benchmark and return parsed summary."""
    # Determine binary name
    if alg.startswith('per_op_'):
        binary = os.path.join(build_dir, f'rundb_{alg}_tpcc')
    else:
        binary = os.path.join(build_dir, f'rundb_{alg}_tpcc')

    cmd = f'{binary} -t{threads} -n{threads} -Tr0 -Ts0 {extra_flags}'
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        return None
    return parse_output(result.stdout)

def main():
    parser = argparse.ArgumentParser(description='Calibrate benchmark duration')
    parser.add_argument('--alg', default='no_wait', help='Algorithm to test')
    parser.add_argument('--threads', type=int, default=8, help='Thread count')
    parser.add_argument('--runs', type=int, default=5, help='Runs per configuration')
    parser.add_argument('--txn-values', default='10000,50000,100000,200000,500000',
                        help='Comma-separated MAX_TXN_PER_PART values')
    parser.add_argument('--warmup', type=int, default=0, help='WARMUP value')
    parser.add_argument('--build-dir', default='build_calibrate')
    args = parser.parse_args()

    txn_values = [int(x) for x in args.txn_values.split(',')]

    print(f'algorithm: {args.alg}, threads: {args.threads}, '
          f'warmup: {args.warmup}, runs: {args.runs}')
    print()
    print(f'{"max_txn":>10} {"run":>4} {"txn_cnt":>10} {"sim_time":>10} '
          f'{"throughput":>12} {"abort_cnt":>10}')
    print('-' * 62)

    results = {}  # max_txn -> list of throughputs

    for max_txn in txn_values:
        print(f'\nBuilding with MAX_TXN_PER_PART={max_txn}, WARMUP={args.warmup}...',
              file=sys.stderr)
        build(args.build_dir, max_txn, args.warmup)

        results[max_txn] = []
        for run in range(args.runs):
            summary = run_one(args.build_dir, args.alg, args.threads)
            if summary is None:
                print(f'{max_txn:>10} {run+1:>4}   TIMEOUT/ERROR')
                continue
            tput = summary['txn_cnt'] / summary['sim_time'] if summary['sim_time'] > 0 else 0
            results[max_txn].append(tput)
            print(f'{max_txn:>10} {run+1:>4} {summary["txn_cnt"]:>10.0f} '
                  f'{summary["sim_time"]:>10.4f} {tput:>12.0f} '
                  f'{summary["abort_cnt"]:>10.0f}')

    # Summary table
    print()
    print(f'{"max_txn":>10} {"mean_tput":>12} {"stddev":>10} {"CV%":>8} {"n":>4}')
    print('-' * 50)
    for max_txn in txn_values:
        tputs = results.get(max_txn, [])
        if len(tputs) < 2:
            print(f'{max_txn:>10} {"N/A":>12} {"N/A":>10} {"N/A":>8} {len(tputs):>4}')
            continue
        mean = sum(tputs) / len(tputs)
        stddev = math.sqrt(sum((t - mean)**2 for t in tputs) / (len(tputs) - 1))
        cv = (stddev / mean * 100) if mean > 0 else 0
        print(f'{max_txn:>10} {mean:>12.0f} {stddev:>10.0f} {cv:>7.1f}% {len(tputs):>4}')

if __name__ == '__main__':
    main()
