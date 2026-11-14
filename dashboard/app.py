#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
博物馆票务预约实时数据看板 - Flask 后端
数据来源:Redis(实时指标) + MySQL(告警明细/统计)
"""
import json
import os
import time

import pymysql
import redis
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MYSQL_CONF = dict(
    host=os.getenv("MYSQL_HOST", "localhost"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", "root123"),
    database=os.getenv("MYSQL_DB", "museum"),
    charset="utf8mb4",
)

rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                  socket_connect_timeout=2, socket_timeout=2)

# MySQL 连接复用(单连接 + 保活,看板查询低频无需连接池)
_mysql = None


def mysql_conn():
    global _mysql
    try:
        if _mysql is not None:
            _mysql.ping(reconnect=True)
            return _mysql
    except Exception:
        _mysql = None
    # autocommit: 只读查询也要及时结束事务,否则长连接会一直持有元数据锁,
    # 演示前 TRUNCATE 清表会被卡住
    _mysql = pymysql.connect(**MYSQL_CONF, autocommit=True)
    return _mysql


def hgetall_json(key):
    """读取 Redis hash,value 为 JSON 字符串"""
    try:
        raw = rds.hgetall(key)
    except Exception:
        return {}
    out = {}
    for k, v in raw.items():
        try:
            out[k] = json.loads(v)
        except (json.JSONDecodeError, TypeError):
            out[k] = v
    return out


# ---------------- 页面 ----------------
@app.route("/")
def index():
    return render_template("index.html")


# ---------------- 实时指标 ----------------
@app.route("/api/metrics")
def metrics():
    """一次性返回看板所需全部指标"""
    data = {"ts": int(time.time() * 1000)}

    # QPS 与总量
    try:
        qps_raw = rds.get("metric:qps")
        qps = json.loads(qps_raw) if qps_raw else {}
    except Exception:
        qps = {}
    # 实时 QPS 用 10 秒窗(每次刷新都在跳动),分钟窗保留总量
    try:
        q10_raw = rds.get("metric:qps10s")
        q10 = json.loads(q10_raw) if q10_raw else {}
    except Exception:
        q10 = {}
    data["qps"] = {
        "count": qps.get("count", 0),
        "qps": round(q10.get("count", 0) / 10.0, 1),
        "qps_1m": round(qps.get("count", 0) / 60.0, 1),
        "ticketSum": qps.get("ticketSum", 0),
        "successRate": qps.get("successRate", 0),
        "windowEnd": qps.get("windowEnd", 0),
    }

    # 趋势(最近 60 分钟点)
    trend = []
    try:
        for member in rds.zrange("metric:trend:1m", -60, -1):
            ts_s, cnt = member.rsplit(":", 1)
            trend.append({"ts": int(ts_s), "count": int(cnt)})
    except Exception:
        pass
    data["trend"] = trend

    # 10 秒级实时流量(最近 180 点 = 30 分钟)
    trend10s = []
    try:
        for member in rds.zrange("metric:trend:10s", -180, -1):
            ts_s, cnt = member.rsplit(":", 1)
            trend10s.append({"ts": int(ts_s), "count": int(cnt)})
    except Exception:
        pass
    data["trend10s"] = trend10s

    # 短窗口预测:对最近 12 个点(2 分钟)做最小二乘线性外推,预测未来 6 个点
    forecast = []
    if len(trend10s) >= 6:
        pts = trend10s[-12:]
        n = len(pts)
        xs = list(range(n))
        ys = [p["count"] for p in pts]
        sx, sy = sum(xs), sum(ys)
        sxx = sum(x * x for x in xs)
        sxy = sum(x * y for x, y in zip(xs, ys))
        denom = n * sxx - sx * sx
        slope = (n * sxy - sx * sy) / denom if denom else 0.0
        intercept = (sy - slope * sx) / n
        last_ts = pts[-1]["ts"]
        for i in range(1, 7):
            v = max(0, intercept + slope * (n - 1 + i))
            forecast.append({"ts": last_ts + i * 10000, "count": round(v, 1)})
    data["forecast"] = forecast

    # 各维度分布
    data["venue"] = hgetall_json("metric:venue_5m")
    data["slot"] = hgetall_json("metric:slot")
    data["client"] = hgetall_json("metric:client")
    data["result"] = hgetall_json("metric:result")
    data["ticket_dist"] = hgetall_json("metric:ticket_dist")
    data["pressure"] = hgetall_json("metric:pressure")

    # 热门时段 TOP(由馆区x时段预约量排序得出)
    venueslot = hgetall_json("metric:venueslot")
    top = sorted(
        ({"name": k, "count": v.get("count", 0)} for k, v in venueslot.items() if isinstance(v, dict)),
        key=lambda x: x["count"], reverse=True)[:10]
    data["top"] = top

    # 风险榜 TOP10
    risk = []
    try:
        for member, score in rds.zrevrange("risk:subjects", 0, 9, withscores=True):
            stype, subject = member.split("|", 1)
            risk.append({"type": stype, "subject": subject, "score": int(score)})
    except Exception:
        pass
    data["risk"] = risk

    # 计数器
    def _int(key):
        try:
            return int(rds.get(key) or 0)
        except Exception:
            return 0

    data["invalid_count"] = _int("stats:invalid")
    data["blacklist_hits"] = _int("stats:blacklist_hits")
    data["auto_ban_count"] = _int("stats:auto_ban")
    data["blacklist_size"] = 0
    try:
        data["blacklist_size"] = rds.scard("blacklist:ips") or 0
    except Exception:
        pass

    # 累计处理量与累计告警数(MySQL window_stats / alert_record)
    data["total_processed"] = 0
    data["total_alerts"] = 0
    try:
        conn = mysql_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(req_count),0) FROM window_stats WHERE window_type='qps_1m'")
            data["total_processed"] = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM alert_record")
            data["total_alerts"] = int(cur.fetchone()[0])
    except Exception:
        pass

    return jsonify(data)


# ---------------- 告警 ----------------
@app.route("/api/alerts")
def alerts():
    """最新告警列表(Redis) + 分级统计(MySQL)"""
    latest = []
    try:
        for item in rds.lrange("alerts:latest", 0, 49):
            latest.append(json.loads(item))
    except Exception:
        pass

    level_stats = {}
    RULE_NAMES = {"R1": "单IP高频请求", "R2": "单IP累计高票数", "R3": "成功率显著偏高",
                  "R4": "固定会话高频请求", "R5": "网段请求量突增", "R6": "私网网段聚集访问"}
    try:
        conn = mysql_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT risk_level, COUNT(*) FROM alert_record GROUP BY risk_level")
            level_stats = {row[0]: row[1] for row in cur.fetchall()}
            cur.execute("SELECT rule_code, COUNT(*) FROM alert_record GROUP BY rule_code")
            counts = {r[0]: r[1] for r in cur.fetchall()}
    except Exception:
        counts = {}
    # 全部 6 条规则固定展示,命中 0 次也显示
    rule_stats = [{"rule": code, "name": RULE_NAMES[code], "count": counts.get(code, 0)}
                  for code in RULE_NAMES]

    return jsonify({"latest": latest, "levelStats": level_stats, "ruleStats": rule_stats})


# ---------------- 实时请求流 ----------------
@app.route("/api/feed")
def feed():
    """滚动事件带:最近抽样事件 + 最新告警摘要"""
    events = []
    try:
        for item in rds.lrange("stream:latest", 0, 19):
            events.append(json.loads(item))
    except Exception:
        pass
    return jsonify({"events": events})


# ---------------- 黑名单 ----------------
@app.route("/api/blacklist", methods=["GET"])
def blacklist_get():
    """手动名单 + 自动封禁名单(含剩余封禁时间)

    两类名单分开存储、分开返回,不能再用 "blacklist:ips 减去自动名单" 推断手动名单:
    同一 IP 一旦被自动封禁过,即使人工又加黑,也会被自动名单掩盖 → 手动列表看不到。
    """
    manual, auto = [], []
    try:
        now_ms = time.time() * 1000          # ZSet score 是毫秒级到期时间戳
        auto_pairs = rds.zrange("blacklist:auto", 0, -1, withscores=True)
        # 已到期的自动封禁不再算生效,也不展示(避免"0分0秒"仍挂在封禁列表里);
        # 剩余不足 1 秒的按 1 秒显示,否则前端会停在"0分0秒"
        auto = [{"ip": ip, "remain": max(1, int(round((exp - now_ms) / 1000)))}
                for ip, exp in auto_pairs if exp > now_ms]
        auto.sort(key=lambda x: x["remain"])
        manual = sorted(rds.smembers("blacklist:manual") or [])
    except Exception:
        pass
    effective = set(manual) | {a["ip"] for a in auto}
    return jsonify({"ips": sorted(effective), "manual": manual, "auto": auto})


@app.route("/api/blacklist/stats", methods=["GET"])
def blacklist_stats():
    try:
        return jsonify({"auto_ban_count": int(rds.get("stats:auto_ban") or 0)})
    except Exception:
        return jsonify({"auto_ban_count": 0})


@app.route("/api/blacklist", methods=["POST"])
def blacklist_add():
    ip = (request.json or {}).get("ip", "").strip()
    if not ip:
        return jsonify({"ok": False, "msg": "ip 为空"}), 400
    # 手动名单单独落 key,保证页面"手动封禁"表一定能看到;同时并入生效名单
    rds.sadd("blacklist:manual", ip)
    rds.sadd("blacklist:ips", ip)
    # 人工封禁优先于自动封禁:撤销自动到期记录,避免 10 分钟后被自动解除
    rds.zrem("blacklist:auto", ip)
    return jsonify({"ok": True, "ip": ip})


@app.route("/api/blacklist/<ip>", methods=["DELETE"])
def blacklist_del(ip):
    rds.srem("blacklist:manual", ip)
    rds.srem("blacklist:ips", ip)
    rds.zrem("blacklist:auto", ip)  # 自动封禁也允许手动解除
    return jsonify({"ok": True, "ip": ip})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
