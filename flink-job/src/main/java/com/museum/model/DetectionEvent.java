package com.museum.model;

/**
 * 规则检测命中事件:某主体触发了某条异常规则。
 */
public class DetectionEvent {
    public String ruleCode;     // R1~R6
    public String ruleName;     // 规则名称
    public String subjectType;  // ip / session / subnet
    public String subject;      // 异常主体
    public double metricValue;  // 实测值
    public double threshold;    // 阈值
    public int score;           // 该规则贡献的风险分
    public long windowStartMs;
    public long windowEndMs;

    public DetectionEvent() {}

    public DetectionEvent(String ruleCode, String ruleName, String subjectType, String subject,
                          double metricValue, double threshold, int score,
                          long windowStartMs, long windowEndMs) {
        this.ruleCode = ruleCode;
        this.ruleName = ruleName;
        this.subjectType = subjectType;
        this.subject = subject;
        this.metricValue = metricValue;
        this.threshold = threshold;
        this.score = score;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
    }
}
