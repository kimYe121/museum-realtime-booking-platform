package com.museum.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.museum.config.JobConfig;
import com.museum.model.BookingEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 数据接入与清洗:
 * 1. JSON 解析 + 字段合法性校验,非法数据进侧输出(错误 topic);
 * 2. 黑名单过滤:命中 Redis 黑名单的 IP 直接拦截,进独立侧输出统计。
 */
public class ParseValidateFunction extends ProcessFunction<String, BookingEvent> {

    public static final OutputTag<String> INVALID_TAG =
            new OutputTag<String>("invalid-data") {};
    public static final OutputTag<String> BLACKLIST_TAG =
            new OutputTag<String>("blacklist-hit") {};

    private static final Set<String> RESULTS = new HashSet<>();
    static {
        Collections.addAll(RESULTS, "SUCCESS", "SOLD_OUT", "QUEUED");
    }

    private final JobConfig cfg;
    private transient ObjectMapper mapper;
    private transient Jedis jedis;
    private transient volatile Set<String> blacklist = new HashSet<>();
    private transient long lastRefresh = 0L;

    public ParseValidateFunction(JobConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        mapper = new ObjectMapper();
        try {
            jedis = new Jedis(cfg.redisHost, cfg.redisPort, 2000);
            refreshBlacklist();
        } catch (Exception e) {
            jedis = null; // Redis 短暂不可用不阻塞主流程,下个周期重试
        }
    }

    private void refreshBlacklist() {
        try {
            if (jedis == null) {
                jedis = new Jedis(cfg.redisHost, cfg.redisPort, 2000);
            }
            // 自动封禁到期解除:清理 blacklist:auto 中已过期的 IP
            long now = System.currentTimeMillis();
            java.util.List<String> expired = jedis.zrangeByScore("blacklist:auto", 0, now);
            if (expired != null && !expired.isEmpty()) {
                // 人工封禁优先:同一 IP 若还在手动名单里,不能随自动封禁到期一起放行
                Set<String> manual = jedis.smembers("blacklist:manual");
                for (String ip : expired) {
                    if (manual == null || !manual.contains(ip)) {
                        jedis.srem("blacklist:ips", ip);
                    }
                }
                jedis.zremrangeByScore("blacklist:auto", 0, now);
            }
            Set<String> ips = jedis.smembers("blacklist:ips");
            blacklist = (ips == null) ? new HashSet<>() : new HashSet<>(ips);
            lastRefresh = System.currentTimeMillis();
        } catch (Exception e) {
            try { if (jedis != null) jedis.close(); } catch (Exception ignore) {}
            jedis = null;
        }
    }

    @Override
    public void processElement(String raw, Context ctx, Collector<BookingEvent> out) {
        // 周期刷新黑名单(墙钟)
        if (System.currentTimeMillis() - lastRefresh > cfg.blacklistRefreshMs) {
            refreshBlacklist();
        }

        BookingEvent ev;
        try {
            ev = parse(raw);
        } catch (Exception e) {
            ctx.output(INVALID_TAG, "PARSE_ERROR:" + e.getMessage() + "|" + truncate(raw));
            return;
        }

        String reason = validate(ev);
        if (reason != null) {
            ctx.output(INVALID_TAG, "INVALID:" + reason + "|" + truncate(raw));
            return;
        }

        // 黑名单拦截:不计入正常统计,独立计数
        if (blacklist.contains(ev.ip)) {
            ctx.output(BLACKLIST_TAG, raw);
            return;
        }

        out.collect(ev);
    }

    private BookingEvent parse(String raw) throws Exception {
        JsonNode n = mapper.readTree(raw);
        BookingEvent e = new BookingEvent();
        e.requestId = text(n, "request_id");
        e.eventTime = text(n, "event_time");
        JsonNode ts = n.get("event_ts_ms");
        e.eventTsMs = (ts == null || ts.isNull()) ? -1 : ts.asLong(-1);
        e.ip = text(n, "ip");
        e.clientType = text(n, "client_type");
        e.venue = text(n, "venue");
        e.venueName = text(n, "venue_name");
        e.timeSlot = text(n, "time_slot");
        e.timeSlotName = text(n, "time_slot_name");
        JsonNode tc = n.get("ticket_count");
        e.ticketCount = (tc == null || tc.isNull()) ? -1 : tc.asInt(-1);
        e.result = text(n, "result");
        e.sessionId = text(n, "session_id");
        return e;
    }

    private String validate(BookingEvent e) {
        if (e.requestId == null || e.requestId.isEmpty()) return "request_id 为空";
        if (e.eventTsMs <= 0) return "event_ts_ms 非法";
        if (e.ip == null || e.ip.isEmpty()) return "ip 为空";
        if (e.venue == null || (!e.venue.equals("east") && !e.venue.equals("main"))) return "venue 非法";
        if (e.timeSlot == null || (!e.timeSlot.equals("morning") && !e.timeSlot.equals("afternoon"))) return "time_slot 非法";
        if (e.ticketCount < 1 || e.ticketCount > 10) return "ticket_count 越界";
        if (e.result == null || !RESULTS.contains(e.result)) return "result 非法";
        return null;
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String truncate(String s) {
        return s.length() > 500 ? s.substring(0, 500) : s;
    }

    @Override
    public void close() {
        if (jedis != null) {
            try { jedis.close(); } catch (Exception ignore) {}
        }
    }
}
