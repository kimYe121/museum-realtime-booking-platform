package com.museum.model;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 告警事件:风险评分超阈值且通过冷却去重后输出。
 */
public class AlertEvent {
    public String ruleCode;
    public String ruleName;
    public String subjectType;
    public String subject;
    public double metricValue;
    public double threshold;
    public int riskScore;
    public String riskLevel;    // 低/中/高/严重
    public long windowStartMs;
    public long windowEndMs;
    public long alertTimeMs;    // 告警触发时间(墙钟)

    public static String levelOf(int score) {
        if (score >= 80) return "严重";
        if (score >= 60) return "高";
        if (score >= 40) return "中";
        return "低";
    }

    private static String fmt(long ms) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(ms));
    }

    public String toJson() {
        return String.format(
                "{\"alertTime\":\"%s\",\"ruleCode\":\"%s\",\"ruleName\":\"%s\",\"subjectType\":\"%s\","
                        + "\"subject\":\"%s\",\"metricValue\":%.2f,\"threshold\":%.2f,"
                        + "\"riskScore\":%d,\"riskLevel\":\"%s\"}",
                fmt(alertTimeMs), ruleCode, ruleName, subjectType, subject,
                metricValue, threshold, riskScore, riskLevel);
    }
}
