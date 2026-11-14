#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
端到端冒烟测试:验证平台各链路功能正确性(不依赖肉眼截图)。

覆盖检查项:
  1.  容器集群全部运行
  2.  Kafka topic 存在
  3.  Flink 作业 RUNNING
  4. 看板 API 全部返回 200 且字段齐全
  5. 数据通路:发送测试事件 → Redis 指标出现
  6. 非法数据:发送坏记录 → stats:invalid 增加 + 错误 topic 有消息
  7. 黑名单:加入 → 该 IP 流量被拦截(stats:blacklist_hits 增加) → 解除
  8. 异常检测:构造高频 IP → MySQL 出现该 IP 告警

用法: python smoke_test.py
退出码: 全部通过 0,否则 1
"""
import json
import subprocess
import sys
import time
import urllib.request

import redis as redis_lib
from confluent_kafka import Producer

import flink_job  # 同目录脚本:作业清场与提交

BROKERS = "127.0.0.1:29092"  # 用 127.0.0.1 避免 localhost 解析到 IPv6 ::1 导致连接延迟
TOPIC = "museum_booking_raw"
ERROR_TOPIC = "museum_booking_error"
API = "http://localhost:5000"

passed, failed = [], []


def check(name, ok, detail=""):
    tag = "PASS" if ok else "FAIL"
    print(f"[{tag}] {name}" + (f"  -- {detail}" if detail else ""))
    (passed if ok else failed).append(name)
    return ok


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def redis_cli(*args):
    return docker("exec", "redis", "redis-cli", *args).stdout.strip()


def mysql(sql):
    r = docker("exec", "mysql", "mysql", "-uroot", "-proot123",
               "museum", "--default-character-set=utf8mb4", "-N", "-B", "-e", sql)
    return r.stdout.strip()


_producer = None


def get_producer():
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": BROKERS,
                              "socket.timeout.ms": 10000})
    return _producer


def produce(messages):
    """发送消息并等待全部投递完成"""
    p = get_producer()
    errors = []

    def cb(err, _msg):
        if err is not None:
            errors.append(str(err))

    for m in messages:
        p.produce(TOPIC, value=m.encode("utf-8"), callback=cb)
    p.flush(30)
    if errors:
        print(f"  [warn] {len(errors)} 条消息投递失败: {errors[0]}")
    return len(errors) == 0


def heartbeat(n=2):
    """发送当前时间的正常事件,推动 Watermark 前进,使尾随窗口得以关闭"""
    ts = int(time.time() * 1000)
    produce([make_event(f"203.0.113.{200 + i}", ts, session=f"hb-{ts}-{i}")
             for i in range(n)])


def make_event(ip, ts, session="smoke-session-1", tickets=2, result="SUCCESS"):
    return json.dumps({
        "request_id": f"smoke-{ts}-{ip}",
        "event_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts / 1000)) + ".000",
        "event_ts_ms": ts, "ip": ip, "client_type": "APP-Android",
        "venue": "east", "venue_name": "东馆",
        "time_slot": "morning", "time_slot_name": "上午",
        "ticket_count": tickets, "result": result,
        "traffic_type": "normal", "is_abnormal": False,
        "session_id": session, "ip_req_interval_ms": None,
    }, ensure_ascii=False)


def api_get(path):
    with urllib.request.urlopen(API + path, timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def main():
    print("=" * 60)
    print("博物馆实时平台 · 冒烟测试")
    print("=" * 60)

    # 1. 容器状态
    out = docker("compose", "-p", "museum-rt-platform", "ps",
                 "--format", "{{.Name}} {{.Status}}").stdout
    expected = ["zk1", "zk2", "zk3", "kafka1", "kafka2", "kafka3",
                "flink-jobmanager", "flink-taskmanager1", "flink-taskmanager2",
                "redis", "mysql", "kafdrop", "dashboard"]
    not_up = [c for c in expected if not any(
        line.startswith(c) and "Up" in line for line in out.splitlines())]
    check("容器集群 13 个全部 Up", not not_up, f"异常: {not_up}" if not_up else "")
    if not_up:
        print("容器未全部就绪,终止测试")
        sys.exit(1)

    # 2. 作业清场:取消残留作业后重提,保证测试从干净状态开始
    print("-" * 60)
    print("清理残留作业并重新提交(避免旧作业抢 slot 导致反复重启)")
    print("-" * 60)
    flink_job.stop_all()
    check("Flink 作业重新提交并 RUNNING", flink_job.start() is not None)

    # 3. Topic
    topics = docker("exec", "kafka1", "/opt/kafka/bin/kafka-topics.sh",
                    "--list", "--bootstrap-server", "kafka1:9092").stdout
    check("Kafka topic 存在", TOPIC in topics and ERROR_TOPIC in topics)

    # 3. Flink 作业(上面已重提,这里只做状态确认)
    jobs = docker("exec", "flink-jobmanager", "flink", "list").stdout
    check("Flink 作业 RUNNING", "RUNNING" in jobs)

    # 4. 看板 API
    for path, keys in [("/api/metrics", ["qps", "trend", "trend10s", "venue",
                                        "slot", "client", "result", "ticket_dist",
                                        "pressure", "top", "risk", "forecast"]),
                       ("/api/alerts", ["latest", "levelStats", "ruleStats"]),
                       ("/api/blacklist", ["ips"])]:
        try:
            status, body = api_get(path)
            missing = [k for k in keys if k not in body]
            check(f"API {path}", status == 200 and not missing,
                  f"缺字段: {missing}" if missing else f"HTTP {status}")
        except Exception as e:
            check(f"API {path}", False, str(e))

    rds = redis_lib.Redis(host="localhost", port=16379, decode_responses=True,
                          protocol=2, socket_connect_timeout=3)

    # 5. 数据通路:发送正常事件(当前时间) → 10 秒窗口指标出现
    test_start_ms = int(time.time() * 1000)
    produce([make_event("203.0.113.10", test_start_ms + i * 100) for i in range(50)])
    ok = False
    for _ in range(60):
        time.sleep(1)
        heartbeat()  # 持续推动 Watermark,让窗口关闭
        for member in rds.zrange("metric:trend:10s", -10, -1):
            if int(member.split(":")[0]) >= test_start_ms:
                ok = True
                break
        if ok:
            break
    check("数据通路:事件 → Kafka → Flink → Redis 指标", ok)

    # 6. 非法数据
    inv_before = int(rds.get("stats:invalid") or 0)
    produce(['{"broken_json": ', '{"request_id":"","event_ts_ms":-1}'])
    ok = False
    for _ in range(20):
        time.sleep(1)
        if int(rds.get("stats:invalid") or 0) > inv_before:
            ok = True
            break
    check("非法数据:坏记录被拦截并计数(stats:invalid)", ok)

    # 7. 黑名单全链路
    bl_ip = "198.51.100.66"
    urllib.request.urlopen(urllib.request.Request(
        API + "/api/blacklist", method="POST",
        data=json.dumps({"ip": bl_ip}).encode(),
        headers={"Content-Type": "application/json"}), timeout=5)
    hits_before = int(rds.get("stats:blacklist_hits") or 0)
    ts = int(time.time() * 1000)
    produce([make_event(bl_ip, ts + i * 100) for i in range(20)])
    ok = False
    for _ in range(30):  # 黑名单热加载 5s + 处理时间
        time.sleep(1)
        if int(rds.get("stats:blacklist_hits") or 0) > hits_before:
            ok = True
            break
    check("黑名单:命中 IP 请求被实时拦截计数", ok)
    urllib.request.urlopen(urllib.request.Request(
        API + f"/api/blacklist/{bl_ip}", method="DELETE"), timeout=5)
    check("黑名单:解除后名单移除",
          bl_ip not in (rds.smembers("blacklist:ips") or set()))

    # 8. 异常检测:构造单 IP 高频(当前时间,10 秒内 30 次) → MySQL 出现告警
    atk_ip = "203.0.113.99"
    ts = int(time.time() * 1000)
    produce([make_event(atk_ip, ts + i * 300) for i in range(30)])
    ok = False
    for _ in range(60):
        time.sleep(1)
        heartbeat()  # 推动 Watermark 关闭检测窗口
        n = int(mysql(
            f"SELECT COUNT(*) FROM alert_record WHERE subject='{atk_ip}'") or 0)
        if n > 0:
            ok = True
            break
    check(f"异常检测:高频 IP {atk_ip} 触发告警写入 MySQL", ok)

    # 9. 自动封禁:持续高频 IP 风险分累积到 60 → 作业自动封禁并倒计时
    ban_ip = "203.0.113.66"
    auto_before = int(rds.get("stats:auto_ban") or 0)
    ts = int(time.time() * 1000)
    produce([make_event(ban_ip, ts + i * 150) for i in range(60)])
    banned = False
    for _ in range(60):  # 滑动窗口逐秒触发评分累加,约 6 次命中越过 60 分
        time.sleep(1)
        heartbeat()
        if rds.zscore("blacklist:auto", ban_ip) is not None:
            banned = True
            break
    check(f"自动封禁:高风险 IP {ban_ip} 被作业自动加入黑名单", banned)
    check("自动封禁:计数器 stats:auto_ban 增长",
          int(rds.get("stats:auto_ban") or 0) > auto_before)
    remain = rds.zscore("blacklist:auto", ban_ip)
    remain_s = int(remain / 1000 - time.time()) if remain else 0   # score 为毫秒时间戳
    check("自动封禁:登记 10 分钟到期时间(可自动解除)", remain_s > 400,
          f"剩余 {remain_s}s" if remain else "无到期时间")
    # 清理,避免影响后续演示与手动名单
    rds.srem("blacklist:ips", ban_ip)
    rds.zrem("blacklist:auto", ban_ip)

    print("=" * 60)
    print(f"结果: {len(passed)} 通过, {len(failed)} 失败")
    if failed:
        print("失败项:", ", ".join(failed))
    print("=" * 60)
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
