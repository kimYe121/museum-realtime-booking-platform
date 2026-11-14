package com.museum.sink;

import com.museum.config.JobConfig;
import com.museum.model.AlertEvent;
import com.museum.model.MetricResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * MySQL 输出端:告警记录、窗口统计快照(数据准确性核对用)。
 * 采用逐条插入(窗口结果与告警均为低频),简化连接管理。
 */
public class JdbcSinks {

    public abstract static class Base<T> extends RichSinkFunction<T> {
        protected final JobConfig cfg;
        protected transient Connection conn;

        protected Base(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(cfg.mysqlUrl, cfg.mysqlUser, cfg.mysqlPassword);
        }

        protected Connection conn() {
            try {
                if (conn == null || conn.isClosed() || !conn.isValid(2)) {
                    conn = DriverManager.getConnection(cfg.mysqlUrl, cfg.mysqlUser, cfg.mysqlPassword);
                }
            } catch (Exception e) {
                try { conn = DriverManager.getConnection(cfg.mysqlUrl, cfg.mysqlUser, cfg.mysqlPassword); }
                catch (Exception ignore) {}
            }
            return conn;
        }

        @Override
        public void close() {
            try { if (conn != null) conn.close(); } catch (Exception ignore) {}
        }
    }

    /** 告警记录 → alert_record 表 */
    public static class Alerts extends Base<AlertEvent> {
        private static final String SQL =
                "INSERT INTO alert_record(alert_time, rule_code, rule_name, subject_type, subject,"
                        + " metric_value, threshold, risk_score, risk_level, window_start, window_end)"
                        + " VALUES(?,?,?,?,?,?,?,?,?,?,?)";

        public Alerts(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(AlertEvent a, Context context) {
            try (PreparedStatement ps = conn().prepareStatement(SQL)) {
                ps.setTimestamp(1, new Timestamp(a.alertTimeMs));
                ps.setString(2, a.ruleCode);
                ps.setString(3, a.ruleName);
                ps.setString(4, a.subjectType);
                ps.setString(5, a.subject);
                ps.setDouble(6, a.metricValue);
                ps.setDouble(7, a.threshold);
                ps.setInt(8, a.riskScore);
                ps.setString(9, a.riskLevel);
                ps.setTimestamp(10, new Timestamp(a.windowStartMs));
                ps.setTimestamp(11, new Timestamp(a.windowEndMs));
                ps.executeUpdate();
            } catch (Exception e) {
                // 告警写库失败不阻塞主链路,依赖 Flink checkpoint 与下游 Redis 兜底
            }
        }
    }

    /** 窗口统计快照 → window_stats 表(qps_1m / venue_5m,用于离线核对) */
    public static class WindowStats extends Base<MetricResult> {
        private static final String SQL =
                "INSERT INTO window_stats(window_end, window_type, dim_key, req_count, ticket_sum, success_count)"
                        + " VALUES(?,?,?,?,?,?)";

        public WindowStats(JobConfig cfg) { super(cfg); }

        @Override
        public void invoke(MetricResult r, Context context) {
            try (PreparedStatement ps = conn().prepareStatement(SQL)) {
                ps.setTimestamp(1, new Timestamp(r.windowEndMs));
                ps.setString(2, r.metric);
                ps.setString(3, r.dimKey);
                ps.setLong(4, r.count);
                ps.setLong(5, r.ticketSum);
                ps.setLong(6, r.successCount);
                ps.executeUpdate();
            } catch (Exception ignore) {}
        }
    }
}
