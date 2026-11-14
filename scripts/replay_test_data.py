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
    ap.add_argument("--brokers", default="127.0.0.1:29092")
    ap.add_argument("--topic", default="museum_booking_raw")
    ap.add_argument("--rate", type=float, default=0, help="条/秒,0=全速")
    ap.add_argument("--tail-events", type=int, default=12,
                    help="回放结束后追加的标点事件数(每 10s 一条,推动 Watermark 关闭尾随窗口)")
    args = ap.parse_args()

    producer = Producer({
        "bootstrap.servers": args.brokers,
        "client.id": "museum-replay",
        "linger.ms": 20,
        "compression.type": "lz4",
    })

    total = 0
    last_ts = 0
    t0 = time.perf_counter()
    with open(args.file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                venue = rec.get("venue", "")
                last_ts = max(last_ts, rec.get("event_ts_ms", 0))
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

    # 追加标点事件:把 Watermark 推过最后一条数据 120s,关闭所有尾随窗口
    for i in range(args.tail_events):
        ts = last_ts + (i + 1) * 10000
        tail = json.dumps({
            "request_id": f"tail-{ts}-{i}",
            "event_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts / 1000)) + ".000",
            "event_ts_ms": ts, "ip": f"203.0.113.{240 + (i % 10)}",
            "client_type": "APP-Android", "venue": "east", "venue_name": "东馆",
            "time_slot": "morning", "time_slot_name": "上午",
            "ticket_count": 1, "result": "SUCCESS",
            "traffic_type": "normal", "is_abnormal": False,
            "session_id": f"tail-{ts}", "ip_req_interval_ms": None,
        }, ensure_ascii=False)
        producer.produce(args.topic, key=b"east", value=tail.encode("utf-8"))

    producer.flush(30)
    cost = time.perf_counter() - t0
    print(f"回放完成:共 {total} 条(+{args.tail_events} 标点事件),耗时 {cost:.1f} 秒,"
          f"平均 {total / cost:.0f} 条/秒")


if __name__ == "__main__":
    main()
