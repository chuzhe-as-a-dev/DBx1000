#!/usr/bin/env python3
"""Benchmark driver for PER_OP hook performance evaluation.

Runs TPC-C across multiple algorithms and thread counts.
All logs, raw output, and parsed CSV are stored in a timestamped results folder.

Usage:
  # Conflict-free TPC-C (default: warehouses = threads, zero remote)
  python3 bench.py --threads 1,2,4,8,16,24,48 --runs 3

  # Standard TPC-C with 4 warehouses and default remote probabilities
  python3 bench.py --threads 1,2,4,8,16,24,48 --runs 3 --warehouses 4 \
      --perc-remote-pay 15 --perc-remote-neworder 1

  # Quick sanity check
  python3 bench.py --threads 1 --runs 1 --algs per_op_noop,no_wait
"""

import argparse, csv, datetime, os, re, subprocess, sys

DEFAULT_ALGS = [
    'per_op_noop',   # zero CC overhead (upper bound)
    'per_op_2pl',    # hook-based NO_WAIT 2PL
    'per_op_occ',    # hook-based OCC
    'no_wait',       # baseline NO_WAIT 2PL
    'tictoc',        # baseline TicToc
    'silo',          # baseline Silo
]

CSV_FIELDS = [
    # Input parameters
    'algorithm', 'threads', 'run',
    'max_txn_per_part', 'warmup', 'warehouses', 'perc_remote_pay', 'perc_remote_neworder',
    # Output metrics
    'txn_cnt', 'abort_cnt', 'sim_time', 'throughput',
    'run_time', 'time_man', 'time_index', 'time_cleanup', 'latency',
    'time_wait', 'time_ts_alloc', 'time_abort', 'time_query',
]

def parse_output(output):
    """Parse SimTime from PASS! line and fields from [summary] line."""
    # SimTime (wall-clock nanoseconds)
    sim_m = re.search(r'PASS!\s*SimTime\s*=\s*(\d+)', output)
    if not sim_m:
        return None
    sim_time_ns = int(sim_m.group(1))

    # Summary fields
    sum_m = re.search(r'\[summary\]\s*(.*)', output)
    if not sum_m:
        return None
    fields = {}
    for pair in sum_m.group(1).split(','):
        k, v = pair.strip().split('=')
        fields[k.strip()] = float(v.strip())

    fields['sim_time'] = sim_time_ns / 1e9  # convert to seconds
    return fields

def build(build_dir, max_txn, warmup, log):
    log.write(f'=== BUILD ===\n')
    cmake_cmd = (
        f'cmake -S . -B {build_dir} -DCMAKE_BUILD_TYPE=RelWithDebInfo'
        f' -DMAX_TXN_PER_PART={max_txn} -DWARMUP={warmup}'
    )
    build_cmd = f'cmake --build {build_dir} --parallel'
    log.write(f'$ {cmake_cmd}\n')
    log.write(f'$ {build_cmd}\n\n')
    log.flush()

    print(f'Building in {build_dir}/ (MAX_TXN_PER_PART={max_txn}, WARMUP={warmup})...')
    ret = os.system(f'{cmake_cmd} > /dev/null 2>&1')
    if ret != 0:
        print("ERROR: cmake configure failed", file=sys.stderr)
        sys.exit(1)
    ret = os.system(f'{build_cmd} > /dev/null 2>&1')
    if ret != 0:
        print("ERROR: build failed", file=sys.stderr)
        sys.exit(1)
    print('Build complete.')
    log.write('Build complete.\n\n')
    log.flush()

def run_one(build_dir, alg, threads, warehouses, perc_remote_pay, perc_remote_neworder, log, raw_log):
    """Run a single benchmark. Returns (cmd, summary)."""
    binary = os.path.join(build_dir, f'rundb_{alg}_tpcc')
    if not os.path.exists(binary):
        return None, None

    n_wh = threads if warehouses == 0 else warehouses
    cmd = (f'{binary} -t{threads} -n{n_wh}'
           f' -Tr{perc_remote_pay} -Ts{perc_remote_neworder}')

    log.write(f'$ {cmd}\n')
    log.flush()

    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        log.write('  TIMEOUT\n')
        return cmd, None

    # Log raw output
    raw_log.write(f'=== {cmd} ===\n')
    raw_log.write(result.stdout)
    if result.stderr:
        raw_log.write(f'--- stderr ---\n{result.stderr}')
    raw_log.write('\n')
    raw_log.flush()

    if result.returncode != 0:
        log.write(f'  EXIT CODE {result.returncode}\n')
        return cmd, None

    summary = parse_output(result.stdout)
    return cmd, summary

def main():
    parser = argparse.ArgumentParser(description='PER_OP benchmark driver')
    parser.add_argument('--threads', default='1,2,4,8,16,24,48',
                        help='Comma-separated thread counts')
    parser.add_argument('--runs', type=int, default=3, help='Runs per combination')
    parser.add_argument('--algs', default=None,
                        help='Comma-separated algorithms (default: all)')
    parser.add_argument('--max-txn', type=int, default=100000,
                        help='MAX_TXN_PER_PART (default 100000)')
    parser.add_argument('--warmup', type=int, default=0,
                        help='WARMUP transactions (default 0)')
    parser.add_argument('--warehouses', type=int, default=0,
                        help='Number of warehouses (default 0 = match thread count)')
    parser.add_argument('--perc-remote-pay', type=float, default=0,
                        help='%% payments with remote customer (default 0)')
    parser.add_argument('--perc-remote-neworder', type=float, default=0,
                        help='%% new-order lines from remote warehouse (default 0)')
    parser.add_argument('--build-dir', default='build_perf')
    parser.add_argument('--results-dir', default='results',
                        help='Base directory for results (default: results)')
    parser.add_argument('--skip-build', action='store_true', help='Skip rebuild')
    args = parser.parse_args()

    thread_counts = [int(x) for x in args.threads.split(',')]
    algs = args.algs.split(',') if args.algs else DEFAULT_ALGS

    # Create timestamped results directory
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir = os.path.join(args.results_dir, timestamp)
    os.makedirs(run_dir, exist_ok=True)

    # Open log files
    log_path = os.path.join(run_dir, 'bench.log')
    raw_path = os.path.join(run_dir, 'raw_output.log')
    csv_path = os.path.join(run_dir, 'results.csv')

    log = open(log_path, 'w')
    raw_log = open(raw_path, 'w')

    # Log the invocation command
    invocation = ' '.join(sys.argv)
    log.write(f'Invocation: python3 {invocation}\n')
    log.write(f'Timestamp:  {timestamp}\n')
    log.write(f'Build dir:  {args.build_dir}\n')
    log.write(f'Results:    {run_dir}/\n\n')

    wh_desc = f'{args.warehouses}' if args.warehouses > 0 else '= threads'

    # Log parameters
    log.write(f'=== PARAMETERS ===\n')
    log.write(f'algorithms:            {", ".join(algs)}\n')
    log.write(f'threads:               {", ".join(str(t) for t in thread_counts)}\n')
    log.write(f'runs:                  {args.runs}\n')
    log.write(f'max_txn_per_part:      {args.max_txn}\n')
    log.write(f'warmup:                {args.warmup}\n')
    log.write(f'warehouses:            {wh_desc}\n')
    log.write(f'perc_remote_pay:       {args.perc_remote_pay}\n')
    log.write(f'perc_remote_neworder:  {args.perc_remote_neworder}\n\n')
    log.flush()

    print(f'Results directory: {run_dir}/')

    # Build
    if not args.skip_build:
        build(args.build_dir, args.max_txn, args.warmup, log)

    # Open CSV
    csv_file = open(csv_path, 'w', newline='')
    writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
    writer.writeheader()

    total = len(algs) * len(thread_counts) * args.runs
    done = 0

    log.write(f'=== RUNS ({total} total) ===\n\n')
    log.flush()

    for alg in algs:
        for threads in thread_counts:
            for run in range(args.runs):
                done += 1
                label = f'[{done}/{total}] {alg} t={threads} run={run+1}'
                print(f'{label}...', end=' ', flush=True)

                cmd, summary = run_one(
                    args.build_dir, alg, threads,
                    args.warehouses, args.perc_remote_pay, args.perc_remote_neworder,
                    log, raw_log)

                if summary is None:
                    msg = 'SKIP (missing binary or timeout)'
                    print(msg)
                    log.write(f'  {msg}\n\n')
                    log.flush()
                    continue

                sim_time = summary['sim_time']
                tput = summary['txn_cnt'] / sim_time if sim_time > 0 else 0

                n_wh = threads if args.warehouses == 0 else args.warehouses
                row = {
                    'algorithm': alg,
                    'threads': threads,
                    'run': run + 1,
                    'max_txn_per_part': args.max_txn,
                    'warmup': args.warmup,
                    'warehouses': n_wh,
                    'perc_remote_pay': args.perc_remote_pay,
                    'perc_remote_neworder': args.perc_remote_neworder,
                    'txn_cnt': int(summary['txn_cnt']),
                    'abort_cnt': int(summary['abort_cnt']),
                    'sim_time': f'{sim_time:.6f}',
                    'throughput': f'{tput:.0f}',
                    'run_time': f'{summary["run_time"]:.6f}',
                    'time_man': f'{summary.get("time_man", 0):.6f}',
                    'time_index': f'{summary.get("time_index", 0):.6f}',
                    'time_cleanup': f'{summary.get("time_cleanup", 0):.6f}',
                    'latency': f'{summary.get("latency", 0):.9f}',
                    'time_wait': f'{summary.get("time_wait", 0):.6f}',
                    'time_ts_alloc': f'{summary.get("time_ts_alloc", 0):.6f}',
                    'time_abort': f'{summary.get("time_abort", 0):.6f}',
                    'time_query': f'{summary.get("time_query", 0):.6f}',
                }
                writer.writerow(row)
                csv_file.flush()

                result_line = (f'txn={int(summary["txn_cnt"])} '
                               f'abort={int(summary["abort_cnt"])} '
                               f'tput={tput:.0f}')
                print(result_line)
                log.write(f'  {result_line}\n\n')
                log.flush()

    csv_file.close()
    log.write(f'=== DONE ===\n')
    log.write(f'CSV:        {csv_path}\n')
    log.write(f'Raw output: {raw_path}\n')
    log.close()
    raw_log.close()

    print(f'\nResults:    {csv_path}')
    print(f'Log:        {log_path}')
    print(f'Raw output: {raw_path}')

if __name__ == '__main__':
    main()
