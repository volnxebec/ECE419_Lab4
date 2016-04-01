#!/bin/bash

IN_SCRIPT=testcases/in.test
OUT_SCRIPT=testcases/out.test

echo "Running test-worker IN script"
rm -rf test_worker_in.log; ./test-worker.sh ${IN_SCRIPT} > test_worker_in.log
echo "Running test-worker OUT script"
rm -rf test_worker_out.log; ./test-worker.sh ${OUT_SCRIPT} > test_worker_out.log

echo "Running test-fs IN script"
rm -rf test_fs_in.log; ./test-fs.sh ${IN_SCRIPT} > test_fs_in.log
echo "Running test-fs OUT script"
rm -rf test_fs_out.log; ./test-fs.sh ${OUT_SCRIPT} > test_fs_out.log

echo "Running test-jt IN script"
rm -rf test_jt_in.log; ./test-jt.sh ${IN_SCRIPT} > test_jt_in.log
echo "Running test-jt OUT script"
rm -rf test_jt_out.log; ./test-jt.sh ${OUT_SCRIPT} > test_jt_out.log

echo "Running test-fs-worker IN script"
rm -rf test_fs_worker_in.log; ./test-fs-worker.sh ${IN_SCRIPT} > test_fs_worker_in.log
echo "Running test-fs-worker OUT script"
rm -rf test_fs_worker_out.log; ./test-fs-worker.sh ${OUT_SCRIPT} > test_fs_worker_out.log

echo "Running test-jt-worker IN script"
rm -rf test_jt_worker_in.log; ./test-jt-worker.sh ${IN_SCRIPT} > test_jt_worker_in.log
echo "Running test-jt-worker OUT script"
rm -rf test_jt_worker_out.log; ./test-jt-worker.sh ${OUT_SCRIPT} > test_jt_worker_out.log

echo "Finally DONE, go check results...."
