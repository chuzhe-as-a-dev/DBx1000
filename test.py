#!/usr/bin/env python3

import os, sys, subprocess, datetime, time, signal

build_dir = "build"
algs = ['DL_DETECT', 'NO_WAIT', 'HEKATON', 'SILO', 'TICTOC']
workloads = ['YCSB', 'TPCC', 'TEST']


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


def test_run(binary, alg, workload, test=''):
    app_flags = ""
    if test == 'read_write':
        app_flags = "-Ar -t1"
    if test == 'conflict':
        app_flags = "-Ac -t4"

    cmd = "%s %s" % (binary, app_flags)
    start = datetime.datetime.now()
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    timeout = 10
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

for alg in algs:
    for wl in workloads:
        binary = os.path.join(build_dir, "rundb_%s_%s" % (alg.lower(), wl.lower()))
        if wl == 'TEST':
            test_run(binary, alg, wl, 'read_write')
        else:
            test_run(binary, alg, wl)

os.system("rm -f temp.out")
