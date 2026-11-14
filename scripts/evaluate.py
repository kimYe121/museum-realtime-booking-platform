#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检测效果评估 + 数据准确性核对。

检测评估(以 IP 为粒度):
    检出集合 = 告警中的 ip 主体 ∪ session 主体映射到的 IP
               ∪ 网段告警 /24 内的真实异常 IP
    与 test-data 中 is_abnormal=true 的 IP 集合对比,
    计算 精确率 / 召回率 / 误报率 / F1。

准确性核对:
    离线统计 test-data 的 1 分钟窗口请求数,与 MySQL window_stats(qps_1m)对比,
    计算相对误差。

用法:
    python evaluate.py --file ../data/test-data-2w.txt \
        --mysql-host localhost --mysql-password root123
"""
import argparse
import json
import subprocess
from collections import defaultdict


def load_ground_truth(path):
    abnormal_ips, normal_ips = set(), set()
    session_ip = {}
    ip_subnet = {}
    abnormal_ip_subnets = defaultdict(set)  # subnet -> abnormal ips
    window_counts = defaultdict(int)        # 1分钟窗口(起点 ms) -> 请求数
    venue_window = defaultdict(lambda: defaultdict(int))  # 5分钟窗口 -> venue -> count

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            e = json.loads(line)
            ip, ts = e["ip"], e["event_ts_ms"]
            subnet = ip.rsplit(".", 1)[0]
            ip_subnet[ip] = subnet
            session_ip[e["session_id"]] = ip

            w1 = ts - ts % 60000
            window_counts[w1] += 1
            w5 = ts - ts % 300000
            venue_window[w5][e["venue_name"]] += 1

            if e.get("is_abnormal"):
                abnormal_ips.add(ip)
                abnormal_ip_subnets[subnet].add(ip)
            else:
                normal_ips.add(ip)

    return {
        "abnormal_ips": abnormal_ips,
        "normal_ips": normal_ips - abnormal_ips,
        "session_ip": session_ip,
        "abnormal_ip_subnets": abnormal_ip_subnets,
        "window_counts": window_counts,
        "venue_window": venue_window,
    }


def mysql_query(sql):
    """通过 pymysql 直连容器 MySQL(127.0.0.1:13306)"""
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=13306, user="root",
                           password="root123", database="museum",
                           charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [list(r) for r in cur.fetchall()]
    finally:
        conn.close()


def evaluate_detection(gt, min_alert_id):
    rows = mysql_query(
        f"SELECT subject_type, subject FROM alert_record WHERE id > {min_alert_id}")

    detected_ips = set()
    ip_alerts, session_alerts, subnet_alerts = 0, 0, 0
    for stype, subject in rows:
        if stype == "ip":
            detected_ips.add(subject)
            ip_alerts += 1
        elif stype == "session":
            session_alerts += 1
            ip = gt["session_ip"].get(subject)
            if ip:
                detected_ips.add(ip)
        elif stype == "subnet":
            subnet_alerts += 1
            subnet = subject.replace(".0/24", "")
            # /24 网段告警:该网段内的真实异常 IP 记为检出
            detected_ips |= gt["abnormal_ip_subnets"].get(subnet, set())
        elif stype == "subnet16":
            subnet_alerts += 1
            prefix = subject.replace(".0.0/16", "")
            # /16 网段告警:该 /16 下所有 /24 的真实异常 IP 记为检出
            for sub, ips in gt["abnormal_ip_subnets"].items():
                if sub.startswith(prefix + "."):
                    detected_ips |= ips

    ab, nm = gt["abnormal_ips"], gt["normal_ips"]
    tp = len(detected_ips & ab)
    fp = len(detected_ips & nm)
    fn = len(ab - detected_ips)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    fpr = fp / len(nm) if nm else 0.0

    print("=" * 56)
    print("检测效果评估(IP 粒度,5% 异常比例数据集)")
    print("=" * 56)
    print(f"告警条数: ip={ip_alerts} session={session_alerts} subnet={subnet_alerts}")
    print(f"真实异常 IP 数 : {len(ab)}")
    print(f"检出异常 IP 数 : {tp}  (误报正常 IP 数: {fp}, 漏报: {fn})")
    print(f"精确率 Precision : {precision:.4f}")
    print(f"召回率 Recall    : {recall:.4f}")
    print(f"F1               : {f1:.4f}")
    print(f"误报率 FPR       : {fpr:.4f}")
    print("=" * 56)
    return {"precision": precision, "recall": recall, "f1": f1, "fpr": fpr,
            "tp": tp, "fp": fp, "fn": fn}


def evaluate_accuracy(gt):
    """离线 1 分钟窗口计数 vs window_stats(qps_1m)"""
    rows = mysql_query(
        "SELECT window_end, req_count FROM window_stats WHERE window_type='qps_1m'")
    db = {}
    import datetime
    for wend, cnt in rows:
        # window_end 是窗口结束时间,对应离线窗口起点 = end - 60000
        # JDBC 按 UTC 写入 DATETIME,这里按 UTC 解释回 epoch
        if isinstance(wend, str):
            wend = datetime.datetime.strptime(wend, "%Y-%m-%d %H:%M:%S.%f")
        ms = int(wend.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000) - 60000
        db[ms] = int(cnt)

    diffs = []
    for w, expect in gt["window_counts"].items():
        if w in db and expect > 50:  # 跳过不完整的首尾窗口
            actual = db[w]
            diffs.append(abs(actual - expect) / expect)

    if not diffs:
        print("[准确性] 无重叠窗口可比对(可能回放数据与统计窗口未对齐)")
        return None
    max_err = max(diffs)
    avg_err = sum(diffs) / len(diffs)
    print(f"[准确性] 比对窗口数: {len(diffs)}, 平均相对误差: {avg_err:.4%}, 最大相对误差: {max_err:.4%}")
    print(f"[准确性] 要求误差 <= 0.5% : {'通过' if max_err <= 0.005 else '未通过'}")
    return {"avg_err": avg_err, "max_err": max_err, "windows": len(diffs)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--min-alert-id", type=int, default=0,
                    help="只统计 id 大于该值的告警(用于区分历史告警)")
    ap.add_argument("--skip-accuracy", action="store_true")
    args = ap.parse_args()

    gt = load_ground_truth(args.file)
    evaluate_detection(gt, args.min_alert_id)
    if not args.skip_accuracy:
        evaluate_accuracy(gt)


if __name__ == "__main__":
    main()
