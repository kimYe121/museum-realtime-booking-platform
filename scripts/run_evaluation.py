#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检测效果评估编排:重提作业(重置 Watermark)→ 清库 → 回放标注数据 → 评估。

为什么要重提作业:
    作业若已消费过当前时间的数据,Watermark 已推进到"现在",
    回放的历史数据会被判定为迟到数据丢弃,导致评估结果全为 0。
    重提作业后从最新 offset 开始消费,Watermark 由回放数据自身推进。
    同时先取消残留作业,避免旧作业抢 slot 造成新作业反复重启。

用法: python run_evaluation.py
"""
import subprocess
import sys
import time

import flink_job  # 同目录脚本:作业清场与提交

PY = sys.executable
BASE = __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0]
DATA = BASE + "\\data\\test-data-2w.txt"


def sh(cmd, show=True):
    r = subprocess.run(cmd, capture_output=True, text=True, shell=isinstance(cmd, str))
    if show and r.stdout.strip():
        print(r.stdout.strip()[-500:])
    if r.returncode != 0 and r.stderr:
        print(r.stderr.strip()[-300:])
    return r


def main():
    print("=" * 60)
    print("步骤 1/5:取消残留作业并重提(重置 Watermark)")
    print("=" * 60)
    flink_job.stop_all()
    if flink_job.start() is None:
        print("作业提交失败,终止评估")
        sys.exit(1)

    print("=" * 60)
    print("步骤 2/5:清空告警与统计表、清空 Redis")
    print("=" * 60)
    sh('docker exec mysql mysql -uroot -proot123 museum -e '
       '"TRUNCATE alert_record; TRUNCATE window_stats;"', show=False)
    sh("docker exec redis redis-cli flushdb", show=False)
    time.sleep(3)

    print("=" * 60)
    print("步骤 3/5:回放标注测试数据(2 万条,5% 异常比例)")
    print("=" * 60)
    r = subprocess.run([PY, BASE + "\\scripts\\replay_test_data.py",
                        "--file", DATA], capture_output=True, text=True)
    print(r.stdout.strip())
    if r.returncode != 0:
        print(r.stderr)
        sys.exit(1)

    print("=" * 60)
    print("步骤 4/5:等待窗口关闭与告警落库(70 秒)")
    print("=" * 60)
    time.sleep(70)

    print("=" * 60)
    print("步骤 5/5:评估检测效果与数据准确性")
    print("=" * 60)
    r = subprocess.run([PY, BASE + "\\scripts\\evaluate.py", "--file", DATA],
                       capture_output=True, text=True)
    print(r.stdout)
    if r.stderr:
        print(r.stderr)


if __name__ == "__main__":
    main()
