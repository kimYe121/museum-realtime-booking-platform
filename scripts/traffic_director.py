#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
流量编排器:驱动课程配套生成器,模拟博物馆真实业务流量节奏。

不修改生成器字段语义,仅通过周期性调整 --rate 实现流量曲线编排:
    低峰平稳(抖动) → 放票预热(线性爬升) → 放票高峰(脉冲尖峰)
    → 高峰回落 → 平稳期,循环往复。

用法(Windows 下运行,向 WSL 容器集群中的 Kafka 发送):
    python traffic_director.py --python D:/anaconda3/python.exe \
        --gen ../generator/museum_booking_gen.py --brokers localhost:29092
"""
import argparse
import math
import random
import signal
import subprocess
import sys
import time

rnd = random.Random(20261114)


def scenario(t, cycle):
    """返回周期第 t 秒的目标速率(条/秒),t ∈ [0, cycle)"""
    phases = [
        (40, lambda p: 60 + rnd.uniform(-15, 25)),                 # 低峰平稳
        (30, lambda p: 100 + 900 * p + rnd.uniform(-40, 80)),      # 预热爬升 →1000
        (45, lambda p: 2600 + rnd.uniform(0, 2200)                  # 放票高峰 2600~4800
                        + 600 * math.sin(p * math.pi * 6)),        #   带脉冲波动
        (25, lambda p: 2000 - 1700 * p + rnd.uniform(-60, 120)),   # 高峰回落 →300
        (35, lambda p: 300 + rnd.uniform(-60, 90)),                # 平稳期
    ]
    acc = 0
    for dur, fn in phases:
        if t < acc + dur:
            return max(20, fn((t - acc) / dur))
        acc += dur
    return 60


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", default=sys.executable, help="Python 解释器路径")
    ap.add_argument("--gen", required=True, help="museum_booking_gen.py 路径")
    ap.add_argument("--brokers", default="localhost:29092")
    ap.add_argument("--topic", default="museum_booking_raw")
    ap.add_argument("--time-scale", type=float, default=1.0)
    ap.add_argument("--adjust-interval", type=float, default=2.0, help="调速间隔(秒)")
    args = ap.parse_args()

    cycle = 40 + 30 + 45 + 25 + 35  # 175 秒一个流量周期
    proc = None

    def stop():
        nonlocal proc
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        proc = None

    def handler(sig, frame):
        stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)

    print("=" * 60)
    print("流量编排器启动:低峰 → 预热 → 放票高峰 → 回落,循环 %d 秒" % cycle)
    print("  Kafka: %s / topic=%s" % (args.brokers, args.topic))
    print("=" * 60)

    t0 = time.perf_counter()
    rate = 60
    try:
        while True:
            t = (time.perf_counter() - t0) % cycle
            rate = int(scenario(t, cycle))
            stop()
            proc = subprocess.Popen([
                args.python, args.gen,
                "--rate", str(rate),
                "--sink", "kafka",
                "--brokers", args.brokers,
                "--topic", args.topic,
                "--time-scale", str(args.time_scale),
                "--quiet",
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("[%s] 周期第 %5.1f 秒 → 目标速率 %5d 条/秒" % (
                time.strftime("%H:%M:%S"), t, rate), flush=True)
            time.sleep(args.adjust_interval)
    finally:
        stop()


if __name__ == "__main__":
    main()
