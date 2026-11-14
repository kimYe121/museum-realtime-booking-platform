package com.museum.function;

import com.museum.config.JobConfig;
import com.museum.model.AlertEvent;
import com.museum.model.DetectionEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 风险评分聚合 + 告警去重冷却:
 * - 同一主体(IP/会话/网段)的多条规则命中按评分累加,取最高分规则作为告警依据;
 * - 评分 >= alertScoreThreshold 触发告警,分级:中(40+)/高(60+)/严重(80+);
 * - 冷却:同一主体在 cooldown(事件时间)内不重复告警;
 * - 所有命中主体的最新评分实时写入下游(Redis 风险榜),即使未达告警线。
 */
public class RiskScoreProcessFunction extends KeyedProcessFunction<String, DetectionEvent, AlertEvent> {

    /** 侧输出:所有主体最新风险分(供 Redis 风险榜展示,含未达告警线主体) */
    public static final OutputTag<String> SCORE_TAG =
            new OutputTag<String>("risk-score") {};

    private final JobConfig cfg;

    /** 最近一次告警的事件时间(冷却依据) */
    private transient ValueState<Long> lastAlertTs;
    /** 当前累计风险分(随窗口滑动缓慢衰减:取历史与本次的较大者) */
    private transient ValueState<Integer> currentScore;

    public RiskScoreProcessFunction(JobConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public void open(Configuration parameters) {
        lastAlertTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastAlertTs", Long.class));
        currentScore = getRuntimeContext().getState(
                new ValueStateDescriptor<>("currentScore", Integer.class));
    }

    @Override
    public void processElement(DetectionEvent d, Context ctx, Collector<AlertEvent> out) throws Exception {
        Integer score = currentScore.value();
        // 评分合成:历史分与本次规则分取较大者后,叠加少量其他规则命中分
        int newScore = (score == null) ? d.score : Math.max(score, d.score) + Math.min(10, d.score / 4);
        currentScore.update(newScore);

        // 实时更新风险榜(侧输出 JSON:subject|score|level)
        ctx.output(SCORE_TAG, d.subjectType + "|" + d.subject + "|" + newScore + "|" + AlertEvent.levelOf(newScore));

        long eventTs = d.windowEndMs;
        Long last = lastAlertTs.value();
        boolean cooledDown = (last == null) || (eventTs - last >= cfg.alertCooldownMs);

        if (newScore >= cfg.alertScoreThreshold && cooledDown) {
            AlertEvent a = new AlertEvent();
            a.ruleCode = d.ruleCode;
            a.ruleName = d.ruleName;
            a.subjectType = d.subjectType;
            a.subject = d.subject;
            a.metricValue = d.metricValue;
            a.threshold = d.threshold;
            a.riskScore = newScore;
            a.riskLevel = AlertEvent.levelOf(newScore);
            a.windowStartMs = d.windowStartMs;
            a.windowEndMs = d.windowEndMs;
            a.alertTimeMs = System.currentTimeMillis();
            out.collect(a);
            lastAlertTs.update(eventTs);
        }
    }
}
