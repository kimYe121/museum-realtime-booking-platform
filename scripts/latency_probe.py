#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
端到端延迟探针。

原理:Flink 每完成一个 10 秒窗口,就把点写入 Redis ZSET metric:trend:10s
(member = windowEndMs:count)。探针持续监听新点出现,
延迟 = 观察到该点的墙钟时间 - 窗口结束时间(事件时间)
     ≈ Watermark 乱序容忍 + 窗口计算 + Redis 写入 + 网络。

用法:
    python latency_probe.py --duration 600 --redis-host localhost
输出:样本数、平均 / P95 / 最大延迟。
"""
import argparse
import statistics
import time

import redis


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=600, help="观测时长(秒)")
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=16379)
    args = ap.parse_args()

    rds = redis.Redis(host=args.redis_host, port=args.redis_port,
                      decode_responses=True, socket_timeout=2, protocol=2)
    seen = set()
    delays = []
    t0 = time.perf_counter()
    print("延迟探针启动,观测 %d 秒 ..." % args.duration)

    while time.perf_counter() - t0 < args.duration:
        try:
            for member in rds.zrange("metric:trend:10s", -20, -1):
                if member in seen:
                    continue
                seen.add(member)
                ts_s = member.split(":")[0]
                wend = int(ts_s)
                delay_ms = int(time.time() * 1000) - wend
                if 0 < delay_ms < 120000:  # 过滤启动前的历史点
                    delays.append(delay_ms)
        except Exception:
            pass
        time.sleep(0.2)

    if not delays:
        print("未采集到样本(确认作业在运行、数据在产生)")
        return
    delays.sort()
    p95 = delays[int(len(delays) * 0.95) - 1]
    print("=" * 50)
    print("样本数   : %d" % len(delays))
    print("平均延迟 : %.0f ms" % statistics.mean(delays))
    print("P95 延迟 : %d ms" % p95)
    print("最大延迟 : %d ms" % delays[-1])
    print("最小延迟 : %d ms" % delays[0])
    print("结论     : 端到端延迟 %s 5 秒要求" % (
        "满足" if p95 <= 5000 else "不满足"))
    print("=" * 50)


if __name__ == "__main__":
    main()
