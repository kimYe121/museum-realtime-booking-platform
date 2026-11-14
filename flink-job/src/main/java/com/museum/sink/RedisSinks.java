package com.museum.sink;

import com.museum.config.JobConfig;
import com.museum.model.AlertEvent;
import com.museum.model.MetricResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis 输出端集合:实时指标、趋势、风险榜、告警列表、计数器。
 * Key 设计见设计文档 7.x。
 */
public class RedisSinks {

    private static JedisPool createPool(JobConfig cfg) {
        JedisPoolConfig pc = new JedisPoolConfig();
        pc.setMaxTotal(8);
        pc.setMaxIdle(4);
        return new JedisPool(pc, cfg.redisHost, cfg.redisPort, 3000);
    }

    /** 通用基类:管理 JedisPool 生命周期 */
    public abstract static class Base<T> extends RichSinkFunction<T> {
        protected final JobConfig cfg;
        protected transient JedisPool pool;

        protected Base(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void open(Configuration parameters) { pool = createPool(cfg); }

        @Override
        public void close() { if (pool != null) pool.close(); }

        protected void safe(JedisOp op) {
            try (Jedis j = pool.getResource()) {
                op.run(j);
            } catch (Exception e) {
                // Redis 抖动不阻塞主链路
            }
        }

        protected interface JedisOp { void run(Jedis j); }
    }

    /** 维度分布指标 → HASH:metric:{type}, field=dimKey, value=JSON */
    public static class MetricHash extends Base<MetricResult> {
        public MetricHash(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(MetricResult r, Context context) {
            safe(j -> j.hset("metric:" + r.metric, r.dimKey, r.toJson()));
            safe(j -> j.set("metric:" + r.metric + ":windowEnd", String.valueOf(r.windowEndMs)));
        }
    }

    /** QPS 指标 → String + 趋势 ZSET(保留最近 240 个点) */
    public static class Qps extends Base<MetricResult> {
        public Qps(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(MetricResult r, Context context) {
            safe(j -> j.set("metric:qps", r.toJson()));
            safe(j -> j.zadd("metric:trend:1m", r.windowEndMs, r.windowEndMs + ":" + r.count));
            safe(j -> j.zremrangeByRank("metric:trend:1m", 0, -241));
            safe(j -> j.set("metric:trend:windowEnd", String.valueOf(r.windowEndMs)));
        }
    }

    /** 10 秒级流量趋势 → ZSET:metric:trend:10s(保留最近 360 点) */
    public static class Trend10s extends Base<MetricResult> {
        public Trend10s(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(MetricResult r, Context context) {
            safe(j -> j.set("metric:qps10s", r.toJson()));
            safe(j -> j.zadd("metric:trend:10s", r.windowEndMs, r.windowEndMs + ":" + r.count));
            safe(j -> j.zremrangeByRank("metric:trend:10s", 0, -361));
        }
    }

    /** 余票压力 JSON → HASH:metric:pressure, field=key */
    public static class Pressure extends Base<String> {
        public Pressure(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(String json, Context context) {
            // json 内含 "key":"east|morning" 字段,提取作为 hash field
            String field = extract(json, "\"key\":\"", "\"");
            safe(j -> j.hset("metric:pressure", field, json));
        }

        private static String extract(String s, String prefix, String suffix) {
            int i = s.indexOf(prefix);
            if (i < 0) return "unknown";
            int start = i + prefix.length();
            int end = s.indexOf(suffix, start);
            return end < 0 ? "unknown" : s.substring(start, end);
        }
    }

    /** 风险榜 → ZSET:risk:subjects, member=subjectType|subject, score=风险分 */
    public static class RiskBoard extends Base<String> {
        public RiskBoard(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(String scoreLine, Context context) {
            String[] parts = scoreLine.split("\\|", 4);
            if (parts.length < 4) return;
            String member = parts[0] + "|" + parts[1];
            double score = Double.parseDouble(parts[2]);
            safe(j -> j.zadd("risk:subjects", score, member));
            // 风险榜只保留前 500,防止膨胀
            safe(j -> j.zremrangeByRank("risk:subjects", 0, -501));
        }
    }

    /**
     * 告警 → LIST:alerts:latest(最新在前,保留 200 条)。
     * 自动封禁不在这里做:告警有 60s 去重冷却,封禁必须即时且可续期,
     * 因此由 RiskScoreProcessFunction 的 BAN_TAG 侧输出交给 AutoBan 处理。
     */
    public static class Alerts extends Base<AlertEvent> {
        public Alerts(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(AlertEvent a, Context context) {
            safe(j -> j.lpush("alerts:latest", a.toJson()));
            safe(j -> j.ltrim("alerts:latest", 0, 199));
        }
    }

    /**
     * 自动封禁 → blacklist:ips(Set) + blacklist:auto(ZSet,score=到期时间戳)。
     * 重复指令只续期不重复计数;到期由 ParseValidateFunction 的周期刷新解除。
     */
    public static class AutoBan extends Base<String> {
        public AutoBan(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(String line, Context context) {
            String[] parts = line.split("\\|", 2);
            if (parts.length < 2 || parts[0].isEmpty()) return;
            String ip = parts[0];
            long expireAt = System.currentTimeMillis() + cfg.autoBanDurationMs;
            safe(j -> {
                // 人工封禁优先:已在手动名单中的 IP 不再写自动到期记录,避免 10 分钟后被自动放行
                if (Boolean.TRUE.equals(j.sismember("blacklist:manual", ip))) return;
                Double prev = j.zscore("blacklist:auto", ip);
                j.sadd("blacklist:ips", ip);
                j.zadd("blacklist:auto", expireAt, ip);
                if (prev == null) {
                    j.incr("stats:auto_ban");   // 仅首次封禁计数
                }
            });
        }
    }

    /** 实时请求流:抽样事件 → LIST:stream:latest(保留 30 条,供大屏滚动事件带) */
    public static class EventFeed extends Base<com.museum.model.BookingEvent> {
        public EventFeed(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(com.museum.model.BookingEvent e, Context context) {
            String json = String.format(
                    "{\"t\":\"%s\",\"v\":\"%s\",\"s\":\"%s\",\"c\":\"%s\",\"n\":%d,\"r\":\"%s\"}",
                    e.eventTime.length() >= 19 ? e.eventTime.substring(11, 19) : e.eventTime,
                    e.venueName, e.timeSlotName, e.clientType, e.ticketCount, e.result);
            safe(j -> j.lpush("stream:latest", json));
            safe(j -> j.ltrim("stream:latest", 0, 29));
        }
    }

    /** 简单计数器:收到一条记录 INCR 指定 key(非法数据量 / 黑名单拦截量) */
    public static class Counter extends Base<String> {
        private final String key;
        public Counter(JobConfig cfg, String key) { super(cfg); this.key = key; }

        @Override
        public void invoke(String value, Context context) {
            safe(j -> j.incr(key));
        }
    }
}
