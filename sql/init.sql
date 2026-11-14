-- ============================================================
-- 博物馆票务预约实时数据处理平台 - MySQL 初始化
-- ============================================================
CREATE DATABASE IF NOT EXISTS museum DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE museum;

-- 告警记录表:异常检测结果 + 风险评分 + 告警分级
CREATE TABLE IF NOT EXISTS alert_record (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    alert_time    DATETIME(3)     NOT NULL COMMENT '告警触发时间',
    rule_code     VARCHAR(32)     NOT NULL COMMENT '触发规则编码 R1~R5',
    rule_name     VARCHAR(64)     NOT NULL COMMENT '触发规则名称',
    subject_type  VARCHAR(16)     NOT NULL COMMENT '异常主体类型 ip/session/subnet',
    subject       VARCHAR(64)     NOT NULL COMMENT '异常主体(IP/会话/网段)',
    metric_value  DOUBLE          NOT NULL COMMENT '指标实测值',
    threshold     DOUBLE          NOT NULL COMMENT '触发阈值',
    risk_score    INT             NOT NULL COMMENT '风险评分',
    risk_level    VARCHAR(8)      NOT NULL COMMENT '风险等级 低/中/高/严重',
    window_start  DATETIME(3)     NULL COMMENT '统计窗口起点',
    window_end    DATETIME(3)     NULL COMMENT '统计窗口终点',
    PRIMARY KEY (id),
    KEY idx_alert_time (alert_time),
    KEY idx_subject (subject),
    KEY idx_risk_level (risk_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='实时告警记录';

-- 非法数据记录表(清洗模块落地的坏数据)
CREATE TABLE IF NOT EXISTS invalid_record (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    receive_time DATETIME(3)     NOT NULL COMMENT '接收时间',
    raw_data     TEXT            NULL COMMENT '原始报文(截断)',
    reason       VARCHAR(128)    NOT NULL COMMENT '非法原因',
    PRIMARY KEY (id),
    KEY idx_receive_time (receive_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='非法数据落地';

-- 窗口统计快照表(用于与离线核对验证数据准确性)
CREATE TABLE IF NOT EXISTS window_stats (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    window_end    DATETIME(3)     NOT NULL COMMENT '窗口结束时间',
    window_type   VARCHAR(32)     NOT NULL COMMENT '窗口类型 qps_1m/venue_5m/...',
    dim_key       VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '维度键',
    req_count     BIGINT          NOT NULL DEFAULT 0 COMMENT '请求数',
    ticket_sum    BIGINT          NOT NULL DEFAULT 0 COMMENT '票数合计',
    success_count BIGINT          NOT NULL DEFAULT 0 COMMENT '成功数',
    PRIMARY KEY (id),
    KEY idx_window (window_type, window_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='窗口统计快照(准确性核对用)';
