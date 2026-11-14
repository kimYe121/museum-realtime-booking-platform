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
    _mysql = pymysql.connect(**MYSQL_CONF)
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
    rule_stats = {}
    try:
        conn = mysql_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT risk_level, COUNT(*) FROM alert_record GROUP BY risk_level")
            level_stats = {row[0]: row[1] for row in cur.fetchall()}
            cur.execute("SELECT rule_code, rule_name, COUNT(*) FROM alert_record GROUP BY rule_code, rule_name")
            rule_stats = [{"rule": r[0], "name": r[1], "count": r[2]} for r in cur.fetchall()]
    except Exception:
        pass

    return jsonify({"latest": latest, "levelStats": level_stats, "ruleStats": rule_stats})


# ---------------- 黑名单 ----------------
@app.route("/api/blacklist", methods=["GET"])
def blacklist_get():
    try:
        ips = sorted(rds.smembers("blacklist:ips"))
    except Exception:
        ips = []
    return jsonify({"ips": ips})


@app.route("/api/blacklist", methods=["POST"])
def blacklist_add():
    ip = (request.json or {}).get("ip", "").strip()
    if not ip:
        return jsonify({"ok": False, "msg": "ip 为空"}), 400
    rds.sadd("blacklist:ips", ip)
    return jsonify({"ok": True, "ip": ip})


@app.route("/api/blacklist/<ip>", methods=["DELETE"])
def blacklist_del(ip):
    rds.srem("blacklist:ips", ip)
    return jsonify({"ok": True, "ip": ip})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
