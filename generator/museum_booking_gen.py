#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
museum_booking_gen.py
====================================================================
博物馆票务预约请求模拟生成器（实时计算课程大作业配套工具）

从 museum_gen_config.yaml 读取全部业务参数；命令行可覆盖运行时参数。

生成字段（15 个）：
    request_id          请求唯一 ID
    event_time          事件时间（毫秒精度字符串）
    event_ts_ms         事件时间（epoch 毫秒，long，推荐用于 Flink EventTime）
    ip                  客户端 IP
    client_type         客户端类型
    venue / venue_name  馆区编码 / 名称（east/东馆, main/本馆）
    time_slot / time_slot_name  时段编码 / 名称（morning/上午, afternoon/下午）
    ticket_count        本次预约票数
    result              预约结果（SUCCESS / SOLD_OUT / QUEUED）
    traffic_type        流量类型（normal / bot / scalper / burst）
    is_abnormal         是否异常（ground truth 标签，便于验证检测效果）
    session_id          会话 ID（机器人同一 IP 固定会话）
    ip_req_interval_ms  同一 IP 相邻请求间隔（毫秒，首次为 null）

依赖：
    pip install pyyaml
    pip install confluent-kafka    # 仅在 --sink kafka 时需要
====================================================================
"""

import argparse
import hashlib
import json
import os
import random
import sys
import time
from datetime import datetime

try:
    import yaml
except ImportError:
    sys.exit("需要 PyYAML，请先执行: pip install pyyaml")

DEFAULT_CONFIG = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "museum_gen_config.yaml"
)


def load_config(path):
    if not os.path.exists(path):
        sys.exit("配置文件不存在: %s" % path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ==================================================================
# 生成器
# ==================================================================
class BookingGenerator:
    def __init__(self, cfg, seed=None):
        self.cfg = cfg
        self.rnd = random.Random(seed)
        self._seq = 0
        self._fmt_cache = {}
        self._tick_count = 0
        self.run_id = time.strftime("%y%m%d%H%M%S") + "-%03d" % random.randint(0, 999)

        # ---- 业务分布 ----
        self.venues = [(v["code"], v["name"]) for v in cfg["venues"]]
        self.venue_weights = [v["weight"] for v in cfg["venues"]]
        self.venue_key = {v["code"]: v["code"].encode() for v in cfg["venues"]}
        self.slots = [(s["code"], s["name"]) for s in cfg["slots"]]
        self.slot_weights = [s["weight"] for s in cfg["slots"]]
        self.clients = [c["code"] for c in cfg["clients"]]
        self.client_weights = [c["weight"] for c in cfg["clients"]]
        self.results = [r["code"] for r in cfg["results"]]
        self.result_weights = [r["weight"] for r in cfg["results"]]
        self.ticket_counts = [t["value"] for t in cfg["ticket_counts"]]
        self.ticket_weights = [t["weight"] for t in cfg["ticket_counts"]]
        self.ip_prefixes = cfg["ip_prefixes"]

        # ---- 异常配置 ----
        abn = cfg.get("abnormal", {}) or {}
        self.abn_enabled = bool(abn.get("enabled", False))
        self.abn_ratio = float(abn.get("ratio", 0.0))
        self.abn_types = abn.get("types", {}) or {}
        self.abn_type_names = list(self.abn_types.keys())
        self.abn_type_weights = [self.abn_types[t]["weight"] for t in self.abn_type_names]
        self.scalper_subnets = abn.get("scalper_subnets", []) or []
        self.bot_ip_pool_size = int(abn.get("bot_ip_pool_size", 30))
        self.bot_ip_sticky_ratio = float(abn.get("bot_ip_sticky_ratio", 0.85))

        # ---- IP 池与状态 ----
        self.bot_ip_pool = self._gen_bot_ips()
        self.current_bot_ip = None
        self.current_bot_session = None
        self.ip_last_ts = {}

    # ---------- IP 相关 ----------
    def _gen_bot_ips(self):
        ips = []
        for _ in range(self.bot_ip_pool_size):
            prefix = self.rnd.choice(self.ip_prefixes)
            ips.append("%s.%d.%d" % (prefix, self.rnd.randint(1, 254), self.rnd.randint(1, 254)))
        return ips

    def _gen_random_ip(self):
        prefix = self.rnd.choice(self.ip_prefixes)
        return "%s.%d.%d" % (prefix, self.rnd.randint(0, 255), self.rnd.randint(1, 254))

    def _gen_scalper_ip(self):
        if not self.scalper_subnets:
            return self._gen_random_ip()
        subnet = self.rnd.choice(self.scalper_subnets)
        return "%s.%d.%d" % (subnet, self.rnd.randint(0, 255), self.rnd.randint(1, 254))

    def _pick_bot_ip(self):
        """返回 (ip, session_id)，粘性复用当前 IP 以形成高频特征"""
        if self.current_bot_ip and self.rnd.random() < self.bot_ip_sticky_ratio:
            return self.current_bot_ip, self.current_bot_session
        self.current_bot_ip = self.rnd.choice(self.bot_ip_pool)
        self.current_bot_session = "bot-" + hashlib.md5(
            self.current_bot_ip.encode()
        ).hexdigest()[:12]
        return self.current_bot_ip, self.current_bot_session

    def _gen_session_id(self):
        return "%016x" % self.rnd.getrandbits(64)

    # ---------- 时间格式化 ----------
    def _fmt_ms(self, ms):
        sec, mil = divmod(ms, 1000)
        s = self._fmt_cache.get(sec)
        if s is None:
            s = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sec))
            if len(self._fmt_cache) >= 20000:
                self._fmt_cache.clear()
            self._fmt_cache[sec] = s
        return "%s.%03d" % (s, mil)

    # ---------- 异常记录 ----------
    def _build_abnormal(self, ev_ms, abn_type):
        cfg = self.abn_types[abn_type]
        client_pool = cfg.get("client_types") or self.clients
        client = self.rnd.choice(client_pool)
        tc_lo, tc_hi = cfg.get("ticket_count_range", [1, 2])
        ticket = self.rnd.randint(int(tc_lo), int(tc_hi))
        succ_w = float(cfg.get("success_weight", 0.8))
        if self.rnd.random() < succ_w:
            result = "SUCCESS"
        else:
            result = self.rnd.choice(["SOLD_OUT", "QUEUED"])

        venue, venue_name = self.rnd.choices(self.venues, weights=self.venue_weights)[0]
        slot, slot_name = self.rnd.choices(self.slots, weights=self.slot_weights)[0]

        if abn_type == "bot":
            ip, session = self._pick_bot_ip()
        elif abn_type == "scalper":
            ip = self._gen_scalper_ip()
            session = self._gen_session_id()
        else:  # burst 及其他
            ip = self._gen_random_ip()
            session = self._gen_session_id()

        return {
            "event_ts_ms": ev_ms,
            "ip": ip,
            "client_type": client,
            "venue": venue,
            "venue_name": venue_name,
            "time_slot": slot,
            "time_slot_name": slot_name,
            "ticket_count": ticket,
            "result": result,
            "traffic_type": abn_type,
            "is_abnormal": True,
            "session_id": session,
        }

    # ---------- 批量生成 ----------
    def build(self, n, t0_ms, t1_ms):
        span = t1_ms - t0_ms
        dumps = json.dumps
        items = []

        # 1) 决定每条请求是否异常
        if self.abn_enabled and self.abn_ratio > 0:
            is_abn_flags = [self.rnd.random() < self.abn_ratio for _ in range(n)]
        else:
            is_abn_flags = [False] * n
        n_normal = n - sum(is_abn_flags)

        # 2) 批量预生成正常请求的字段（大幅提升高吞吐场景性能）
        norm_venues = self.rnd.choices(self.venues, weights=self.venue_weights, k=n_normal)
        norm_slots = self.rnd.choices(self.slots, weights=self.slot_weights, k=n_normal)
        norm_clients = self.rnd.choices(self.clients, weights=self.client_weights, k=n_normal)
        norm_results = self.rnd.choices(self.results, weights=self.result_weights, k=n_normal)
        norm_tickets = self.rnd.choices(self.ticket_counts, weights=self.ticket_weights, k=n_normal)
        norm_prefixes = self.rnd.choices(self.ip_prefixes, k=n_normal)
        norm_rands = [self.rnd.getrandbits(16) for _ in range(n_normal)]

        norm_idx = 0
        fmt_ms = self._fmt_ms
        ip_last_ts = self.ip_last_ts
        seq = self._seq
        run_id = self.run_id

        for i in range(n):
            ev_ms = t0_ms + span * i // n

            if is_abn_flags[i]:
                abn_type = self.rnd.choices(
                    self.abn_type_names, weights=self.abn_type_weights
                )[0]
                rec = self._build_abnormal(ev_ms, abn_type)
            else:
                venue, venue_name = norm_venues[norm_idx]
                slot, slot_name = norm_slots[norm_idx]
                client = norm_clients[norm_idx]
                result = norm_results[norm_idx]
                ticket = norm_tickets[norm_idx]
                prefix = norm_prefixes[norm_idx]
                r = norm_rands[norm_idx]
                norm_idx += 1
                rec = {
                    "event_ts_ms": ev_ms,
                    "ip": "%s.%d.%d" % (prefix, r >> 8, r & 0xFF),
                    "client_type": client,
                    "venue": venue,
                    "venue_name": venue_name,
                    "time_slot": slot,
                    "time_slot_name": slot_name,
                    "ticket_count": ticket,
                    "result": result,
                    "traffic_type": "normal",
                    "is_abnormal": False,
                    "session_id": "%016x" % self.rnd.getrandbits(64),
                }

            # 计算同一 IP 的请求间隔
            ip = rec["ip"]
            prev = ip_last_ts.get(ip)
            rec["ip_req_interval_ms"] = (ev_ms - prev) if prev is not None else None
            ip_last_ts[ip] = ev_ms

            # 组装输出 JSON（字段顺序固定，便于阅读和调试）
            seq += 1
            ordered = {
                "request_id": "%s-%09d" % (run_id, seq),
                "event_time": fmt_ms(ev_ms),
                "event_ts_ms": ev_ms,
                "ip": rec["ip"],
                "client_type": rec["client_type"],
                "venue": rec["venue"],
                "venue_name": rec["venue_name"],
                "time_slot": rec["time_slot"],
                "time_slot_name": rec["time_slot_name"],
                "ticket_count": rec["ticket_count"],
                "result": rec["result"],
                "traffic_type": rec["traffic_type"],
                "is_abnormal": rec["is_abnormal"],
                "session_id": rec["session_id"],
                "ip_req_interval_ms": rec["ip_req_interval_ms"],
            }
            items.append((
                self.venue_key[rec["venue"]],
                dumps(ordered, ensure_ascii=False, separators=(",", ":")),
            ))

        self._seq = seq

        # 简单内存保护：每 50 个 tick 检查一次 IP 缓存大小
        self._tick_count += 1
        if self._tick_count % 50 == 0 and len(self.ip_last_ts) > 1000000:
            self.ip_last_ts.clear()

        return items


# ==================================================================
# 输出端
# ==================================================================
class StdoutSink:
    name = "stdout"

    def __init__(self):
        self.ok = 0
        self.err = 0

    def emit(self, items):
        sys.stdout.write("\n".join(line for _, line in items) + "\n")
        sys.stdout.flush()
        self.ok += len(items)

    def close(self):
        sys.stdout.flush()


class FileSink:
    name = "file"

    def __init__(self, path):
        self.ok = 0
        self.err = 0
        self.path = path
        d = os.path.dirname(os.path.abspath(path))
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
        self.f = open(path, "a", encoding="utf-8", buffering=1 << 20)

    def emit(self, items):
        self.f.write("\n".join(line for _, line in items) + "\n")
        self.ok += len(items)

    def close(self):
        self.f.flush()
        self.f.close()


class KafkaSink:
    name = "kafka"

    def __init__(self, brokers, topic, compression="lz4"):
        try:
            from confluent_kafka import Producer
        except ImportError:
            sys.exit("未安装 confluent-kafka，请先执行: pip install confluent-kafka")

        self.topic = topic
        self.ok = 0
        self.err = 0
        self.last_err = None
        self.producer = Producer({
            "bootstrap.servers": brokers,
            "client.id": "museum-booking-gen",
            "acks": "1",
            "linger.ms": 50,
            "batch.num.messages": 20000,
            "queue.buffering.max.messages": 500000,
            "queue.buffering.max.kbytes": 1048576,
            "compression.type": compression,
        })

    def _cb(self, err, _msg):
        if err is not None:
            self.err += 1
            self.last_err = str(err)
        else:
            self.ok += 1

    def emit(self, items):
        produce = self.producer.produce
        for key, line in items:
            produce(self.topic, key=key, value=line.encode("utf-8"), callback=self._cb)
        self.producer.poll(0)

    def close(self):
        self.producer.flush(30)


def make_sink(name, args):
    if name == "stdout":
        return StdoutSink()
    if name == "file":
        return FileSink(args.output)
    return KafkaSink(args.brokers, args.topic, args.compression)


# ==================================================================
# 主流程
# ==================================================================
def build_parser():
    p = argparse.ArgumentParser(
        description="博物馆票务预约请求模拟生成器（配置见 museum_gen_config.yaml）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="示例：python3 museum_booking_gen.py --rate high --sink kafka",
    )
    p.add_argument("--config", default=DEFAULT_CONFIG, help="配置文件路径")
    p.add_argument("--rate", default=None,
                   help="覆盖配置中的速率：low/medium/high/xhigh，或直接写数字")
    p.add_argument("--sink", choices=["stdout", "file", "kafka"], default=None,
                   help="覆盖配置中的输出方式")
    p.add_argument("--output", default=None, help="覆盖配置中的 file 输出路径")
    p.add_argument("--brokers", default=None, help="覆盖配置中的 Kafka broker 列表")
    p.add_argument("--topic", default=None, help="覆盖配置中的 Kafka topic")
    p.add_argument("--compression", default=None, help="覆盖 Kafka 压缩算法")
    p.add_argument("--duration", type=float, default=None, help="覆盖运行时长（秒）")
    p.add_argument("--time-scale", type=float, default=None, help="覆盖事件时间加速倍率")
    p.add_argument("--start-time", default=None,
                   help="事件时间起点 'YYYY-MM-DD HH:MM:SS'，默认当前时间")
    p.add_argument("--seed", type=int, default=None, help="随机种子，便于复现")
    p.add_argument("--quiet", action="store_true", help="不打印每秒统计")
    return p


def main():
    args = build_parser().parse_args()
    cfg = load_config(args.config)
    rt = cfg.get("runtime", {}) or {}

    # ---- 合并运行时参数：命令行 > 配置文件 ----
    rate = args.rate if args.rate is not None else rt.get("rate", "medium")
    sink_name = args.sink if args.sink is not None else rt.get("sink", "stdout")
    duration = args.duration if args.duration is not None else rt.get("duration", 0)
    time_scale = args.time_scale if args.time_scale is not None else rt.get("time_scale", 1.0)

    file_cfg = rt.get("file", {}) or {}
    args.output = args.output if args.output is not None else file_cfg.get(
        "output", "museum_booking.jsonl")

    kafka_cfg = rt.get("kafka", {}) or {}
    args.brokers = args.brokers if args.brokers is not None else kafka_cfg.get(
        "brokers", "localhost:9092")
    args.topic = args.topic if args.topic is not None else kafka_cfg.get(
        "topic", "museum_booking")
    args.compression = args.compression if args.compression is not None else kafka_cfg.get(
        "compression", "lz4")

    # ---- 解析速率 ----
    presets = cfg.get("rate_presets", {}) or {}
    if isinstance(rate, str) and rate in presets:
        rate = int(presets[rate])
    else:
        try:
            rate = int(rate)
        except (TypeError, ValueError):
            sys.exit("速率非法: %s" % rate)
    if rate <= 0:
        sys.exit("速率必须大于 0")

    time_scale = float(time_scale)
    if time_scale <= 0:
        sys.exit("time_scale 必须大于 0")
    duration = float(duration)

    tick = float(cfg.get("scheduler", {}).get("tick", 0.1))

    # ---- 起始事件时间 ----
    if args.start_time:
        try:
            dt = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            sys.exit("--start-time 格式应为 'YYYY-MM-DD HH:MM:SS'")
    else:
        dt = datetime.now().replace(microsecond=0)
    base_ms = int(dt.timestamp() * 1000)
    per_tick_ms = max(1, int(round(tick * time_scale * 1000)))

    # ---- 创建生成器与输出端 ----
    gen = BookingGenerator(cfg, seed=args.seed)
    sink = make_sink(sink_name, args)

    # ---- 打印启动信息 ----
    abn = cfg.get("abnormal", {}) or {}
    abn_types = abn.get("types", {}) or {}
    print("=" * 64, file=sys.stderr)
    print("博物馆票务预约数据生成器", file=sys.stderr)
    print("  配置文件   : %s" % args.config, file=sys.stderr)
    print("  目标速率   : %d 条/秒" % rate, file=sys.stderr)
    print("  输出方式   : %s" % sink.name, file=sys.stderr)
    if sink.name == "kafka":
        print("  Kafka      : %s / topic=%s" % (args.brokers, args.topic), file=sys.stderr)
    elif sink.name == "file":
        print("  输出文件   : %s" % args.output, file=sys.stderr)
    print("  事件时间   : %s 起，加速 %.1f 倍，毫秒精度" % (
        dt.strftime("%Y-%m-%d %H:%M:%S"), time_scale), file=sys.stderr)
    print("  运行时长   : %s" % (
        "不限（Ctrl-C 结束）" if duration <= 0 else "%.0f 秒" % duration), file=sys.stderr)
    if gen.abn_enabled and gen.abn_ratio > 0:
        types_desc = ", ".join(
            "%s=%.0f%%" % (t, abn_types[t]["weight"] * 100)
            for t in gen.abn_type_names
        )
        print("  异常模拟   : 开启（比例 %.1f%%；类型：%s）" % (
            gen.abn_ratio * 100, types_desc), file=sys.stderr)
    else:
        print("  异常模拟   : 关闭", file=sys.stderr)
    print("=" * 64, file=sys.stderr)

    # ---- 主循环 ----
    run_start = time.perf_counter()
    report_t = run_start
    report_total = 0
    total = 0
    tick_idx = 0
    acc = 0.0
    last_warn = 0.0

    try:
        while True:
            if duration > 0 and time.perf_counter() - run_start >= duration:
                break

            # 固定节拍调度，长时间运行不漂移
            deadline = run_start + (tick_idx + 1) * tick
            now = time.perf_counter()
            if now < deadline:
                time.sleep(deadline - now)
            elif now - deadline > 1.0 and now - last_warn > 5.0:
                print("[警告] 生成速度跟不上目标速率，实际低于 %d 条/秒" % rate,
                      file=sys.stderr)
                last_warn = now

            # 本 tick 的事件时间区间
            t0_ms = base_ms + tick_idx * per_tick_ms
            t1_ms = t0_ms + per_tick_ms

            # 累加器控制速率，避免浮点误差导致条数抖动
            acc += rate * tick
            n = int(acc)
            acc -= n

            if n > 0:
                sink.emit(gen.build(n, t0_ms, t1_ms))
                total += n

            tick_idx += 1

            # 每秒打印统计
            now = time.perf_counter()
            if not args.quiet and now - report_t >= 1.0:
                elapsed = now - report_t
                delta = total - report_total
                msg = "[%s] 实际 %.0f 条/秒 | 累计 %d 条" % (
                    time.strftime("%H:%M:%S"), delta / elapsed, total)
                if sink.name == "kafka":
                    msg += " | Kafka 成功 %d 失败 %d" % (sink.ok, sink.err)
                    if sink.last_err:
                        msg += " | 最近错误: %s" % sink.last_err
                print(msg, file=sys.stderr)
                report_t = now
                report_total = total

    except KeyboardInterrupt:
        print("\n收到 Ctrl-C，正在停止 ...", file=sys.stderr)
    finally:
        sink.close()
        cost = time.perf_counter() - run_start
        print("-" * 64, file=sys.stderr)
        print("生成结束：共 %d 条，耗时 %.1f 秒，平均 %.0f 条/秒" % (
            total, cost, total / cost if cost > 0 else 0), file=sys.stderr)
        if sink.name == "kafka":
            print("Kafka 投递：成功 %d，失败 %d" % (sink.ok, sink.err), file=sys.stderr)


if __name__ == "__main__":
    main()