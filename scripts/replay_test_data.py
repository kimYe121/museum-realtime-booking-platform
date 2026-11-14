#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回放 test-data-2w.txt 到 Kafka(用于检测效果评估与准确性核对)。
保留原始 event_ts_ms,便于与窗口统计对齐。

用法:
    python replay_test_data.py --file ../data/test-data-2w.txt \
        --brokers localhost:29092 --topic museum_booking_raw --rate 5000
"""
import argparse
import json
import time

from confluent_kafka import Producer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--brokers", default="localhost:29092")
    ap.add_argument("--topic", default="museum_booking_raw")
    ap.add_argument("--rate", type=float, default=0, help="条/秒,0=全速")
    args = ap.parse_args()

    producer = Producer({
        "bootstrap.servers": args.brokers,
        "client.id": "museum-replay",
        "linger.ms": 20,
        "compression.type": "lz4",
    })

    total = 0
    t0 = time.perf_counter()
    with open(args.file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                venue = json.loads(line).get("venue", "")
            except json.JSONDecodeError:
                venue = ""
            producer.produce(args.topic, key=venue.encode(), value=line.encode("utf-8"))
            total += 1
            if args.rate > 0:
                expect = total / args.rate
                cost = time.perf_counter() - t0
                if expect > cost:
                    time.sleep(expect - cost)
            if total % 5000 == 0:
                producer.poll(0)
                print(f"已发送 {total} 条 ...", flush=True)

    producer.flush(30)
    cost = time.perf_counter() - t0
    print(f"回放完成:共 {total} 条,耗时 {cost:.1f} 秒,平均 {total / cost:.0f} 条/秒")


if __name__ == "__main__":
    main()
