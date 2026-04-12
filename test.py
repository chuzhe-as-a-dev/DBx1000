#!/usr/bin/env python3

import os, sys, subprocess, datetime, time, signal

build_dir = "build"

# Baseline algorithms
algs = ['DL_DETECT', 'NO_WAIT', 'HEKATON', 'SILO', 'TICTOC']
workloads = ['YCSB', 'TPCC', 'TEST']

# PER_OP hook variants — each links a different cc_hooks_*.cpp.
# TEST workload is skipped: it calls txn_man::get_row() directly, bypassing
# cc_pre_op/cc_post_op hooks. See NOTES.md for details.
# MVCC TPCC is skipped: heavy per-row version history init makes TPCC too slow.
per_op_variants = ['NOOP', 'NO_WAIT', 'OCC', 'MVCC', 'PJ_TPCC_4WH']
per_op_workloads = ['YCSB', 'TPCC']
per_op_skip = {'MVCC': {'TPCC'}, 'PJ_TPCC_4WH': {'YCSB'}}


def build_all():
    ret = os.system(
        'cmake -S . -B %s -DCMAKE_BUILD_TYPE=Debug'
        ' -DDBX_TEST_MODE=ON'
        ' > temp.out 2>&1' % build_dir
    )
    if ret != 0:
        print("ERROR: cmake configure failed (see temp.out)")
        sys.exit(1)
    ret = os.system("cmake --build %s --parallel > temp.out 2>&1" % build_dir)
    if ret != 0:
        print("ERROR: build failed (see temp.out)")
        sys.exit(1)
    print("PASS Build\t\tall variants")


def test_run(binary, alg, workload, test='', timeout_sec=10):
    app_flags = ""
    if test == 'read_write':
        app_flags = "-Ar -t1"
    if test == 'conflict':
        app_flags = "-Ac -t4"

    cmd = "%s %s" % (binary, app_flags)
    start = datetime.datetime.now()
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    timeout = timeout_sec
    while process.poll() is None:
        time.sleep(1)
        if (datetime.datetime.now() - start).seconds > timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            print("ERROR. Timeout cmd=%s" % cmd)
            sys.exit(1)
    if b"PASS" in process.stdout.read():
        label = "(%s)" % test if test else ""
        print("PASS execution. \talg=%s,\tworkload=%s%s" % (alg, workload, label))
        return
    print("FAILED execution. cmd=%s" % cmd)
    sys.exit(1)


build_all()

# Baseline algorithms
for alg in algs:
    for wl in workloads:
        binary = os.path.join(build_dir, "rundb_%s_%s" % (alg.lower(), wl.lower()))
        if wl == 'TEST':
            test_run(binary, alg, wl, 'read_write')
        else:
            test_run(binary, alg, wl)

# PER_OP variants
for variant in per_op_variants:
    alg_name = "PER_OP_%s" % variant
    for wl in per_op_workloads:
        if wl in per_op_skip.get(variant, set()):
            print("SKIP execution. \talg=%s,\tworkload=%s (prototype limitation)" % (alg_name, wl))
            continue
        suffix = "_pj" if variant.lower().startswith("pj_") and wl == "TPCC" else ""
        binary = os.path.join(build_dir, "rundb_per_op_%s_%s%s" % (variant.lower(), wl.lower(), suffix))
        test_run(binary, alg_name, wl)

os.system("rm -f temp.out")
