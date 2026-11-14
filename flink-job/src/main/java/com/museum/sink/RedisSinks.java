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

    /** 告警 → LIST:alerts:latest(最新在前,保留 200 条) */
    public static class Alerts extends Base<AlertEvent> {
        public Alerts(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(AlertEvent a, Context context) {
            safe(j -> j.lpush("alerts:latest", a.toJson()));
            safe(j -> j.ltrim("alerts:latest", 0, 199));
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
