#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink 作业生命周期管理(每个脚本运行前先清场,避免作业叠加与重启风暴)。

为什么需要:
    作业异常重启时会持续刷错误日志;若上一次的作业没取消就直接重提,
    新旧作业会同时抢 TaskManager slot,导致新作业一直 INITIALIZING、
    旧作业在 RESTARTING 之间反复刷栈,看起来"报一堆错误"。

用法:
    python flink_job.py status     # 查看作业状态
    python flink_job.py stop       # 取消全部运行中/重启中的作业
    python flink_job.py start      # 提交作业并等待 RUNNING
    python flink_job.py ensure     # stop + start(推荐,提交前清场)
"""
import re
import subprocess
import sys
import time

JM = "flink-jobmanager"
JAR = "/opt/jobs/museum-job.jar"
MAIN_CLASS = "com.museum.BookingJob"
JOB_NAME = "museum-booking-realtime-job"

# flink list 行格式: 14.11.2026 07:25:07 : 4b92ac26043e63ce33452eb607f28131 : 作业名 (RUNNING)
JOB_ID_RE = re.compile(r":\s*([0-9a-f]{32})\s*:")


def sh(*args, timeout=180):
    """在 JobManager 容器内执行 flink 命令"""
    return subprocess.run(["docker", "exec", JM, *args], capture_output=True, text=True,
                          encoding="utf-8", errors="ignore", timeout=timeout)


def running_jobs():
    """返回 [(job_id, state), ...],只包含运行中/重启中的作业"""
    out = sh("flink", "list").stdout
    jobs = []
    for line in out.splitlines():
        m = JOB_ID_RE.search(line)
        if not m:
            continue
        state = "RUNNING" if "RUNNING" in line else (
            "RESTARTING" if "RESTARTING" in line else "OTHER")
        jobs.append((m.group(1), state))
    return jobs


def stop_all():
    """取消全部作业并等待 slot 释放,幂等"""
    jobs = running_jobs()
    if not jobs:
        print("  [flink] 当前没有运行中的作业")
        return 0
    for job_id, state in jobs:
        print(f"  [flink] 取消作业 {job_id}({state})")
        sh("flink", "cancel", job_id)
    # cancel 是异步的,等待作业真正退出、slot 归还
    for _ in range(30):
        time.sleep(2)
        if not running_jobs():
            break
    left = running_jobs()
    print("  [flink] 作业已清空" if not left else f"  [flink] 仍有 {len(left)} 个作业未退出")
    return len(jobs)


def start(wait_sec=120):
    """提交作业并等待进入 RUNNING,返回 job_id(失败返回 None)"""
    r = sh("flink", "run", "-d", "-c", MAIN_CLASS, JAR)
    if r.returncode != 0:
        print("  [flink] 提交失败:" + (r.stderr or r.stdout).strip()[-400:])
        return None
    m = re.search(r"JobID\s+([0-9a-f]{32})", r.stdout + r.stderr)
    job_id = m.group(1) if m else None
    deadline = time.time() + wait_sec
    while time.time() < deadline:
        time.sleep(2)
        jobs = running_jobs()
        if any(state == "RUNNING" for _, state in jobs):
            time.sleep(5)  # 等 Kafka Source / 状态后端初始化完成
            print(f"  [flink] 作业已 RUNNING:{job_id or jobs[0][0]}")
            return job_id or jobs[0][0]
    print("  [flink] 等待作业进入 RUNNING 超时")
    return None


def status():
    jobs = running_jobs()
    if not jobs:
        print("  无运行中作业")
    for job_id, state in jobs:
        print(f"  {job_id}  {state}  {JOB_NAME}")
    return jobs


def main():
    action = sys.argv[1] if len(sys.argv) > 1 else "status"
    if action == "stop":
        stop_all()
    elif action == "start":
        sys.exit(0 if start() else 1)
    elif action == "ensure":
        stop_all()
        sys.exit(0 if start() else 1)
    else:
        status()


if __name__ == "__main__":
    main()