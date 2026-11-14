package com.museum.config;

import java.io.Serializable;

/**
 * 作业配置:全部支持环境变量覆盖,便于 docker-compose 编排调参。
 */
public class JobConfig implements Serializable {

    // ---- Kafka ----
    public String kafkaBrokers = env("KAFKA_BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092");
    public String topicRaw = env("TOPIC_RAW", "museum_booking_raw");
    public String topicError = env("TOPIC_ERROR", "museum_booking_error");
    public String consumerGroup = env("CONSUMER_GROUP", "museum-flink-group");

    // ---- Redis ----
    public String redisHost = env("REDIS_HOST", "redis");
    public int redisPort = Integer.parseInt(env("REDIS_PORT", "6379"));

    // ---- MySQL ----
    public String mysqlUrl = env("MYSQL_URL", "jdbc:mysql://mysql:3306/museum?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8&rewriteBatchedStatements=true");
    public String mysqlUser = env("MYSQL_USER", "root");
    public String mysqlPassword = env("MYSQL_PASSWORD", "root123");

    // ---- 乱序与窗口 ----
    public long outOfOrderSec = Long.parseLong(env("OUT_OF_ORDER_SEC", "5"));

    // ---- 余票压力:每个馆区x时段的假设总库存(票数) ----
    public int slotCapacity = Integer.parseInt(env("SLOT_CAPACITY", "10000"));

    // ---- 异常检测阈值 ----
    /** R1 单 IP 10 秒滑动窗口请求数阈值 */
    public int ipFreqThreshold = Integer.parseInt(env("DETECT_IP_FREQ", "15"));
    /** R2 单 IP 60 秒窗口累计票数阈值 */
    public int ipTicketThreshold = Integer.parseInt(env("DETECT_IP_TICKETS", "40"));
    /** R3 成功率异常:窗口内最少请求数 */
    public int succMinReq = Integer.parseInt(env("DETECT_SUCC_MIN_REQ", "10"));
    /** R3 成功率异常:成功率阈值(全局基线约 0.86) */
    public double succRateThreshold = Double.parseDouble(env("DETECT_SUCC_RATE", "0.95"));
    /** R4 固定会话 10 秒窗口请求数阈值 */
    public int sessionFreqThreshold = Integer.parseInt(env("DETECT_SESSION_FREQ", "10"));
    /** R5 同 /24 网段 10 秒窗口请求数阈值 */
    public int subnetFreqThreshold = Integer.parseInt(env("DETECT_SUBNET_FREQ", "30"));

    // ---- 风险评分与告警 ----
    /** 告警触发评分阈值(达到"中"级) */
    public int alertScoreThreshold = Integer.parseInt(env("ALERT_SCORE_THRESHOLD", "40"));
    /** 同一主体告警冷却时间(事件时间,毫秒) */
    public long alertCooldownMs = Long.parseLong(env("ALERT_COOLDOWN_MS", "60000"));

    // ---- 黑名单刷新周期(墙钟毫秒) ----
    public long blacklistRefreshMs = Long.parseLong(env("BLACKLIST_REFRESH_MS", "5000"));

    public static JobConfig fromEnv() {
        return new JobConfig();
    }

    protected static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }
}
