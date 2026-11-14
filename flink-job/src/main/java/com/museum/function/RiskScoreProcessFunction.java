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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 风险评分聚合 + 告警去重冷却:
 * - 评分按"命中规则种类"累加:同一规则重复命中只刷新时间,不重复加分,上限 100;
 * - 评分 >= alertScoreThreshold 触发告警,分级:中(40+)/高(60+)/严重(80+);
 * - 冷却:同一主体在 cooldown(事件时间)内不重复告警;
 * - 所有命中主体的最新评分实时写入下游(Redis 风险榜),即使未达告警线。
 *
 * 为什么同一规则不重复累加:
 *   检测窗口每秒滑动一次,一条规则在 10 秒内会对同一主体输出多次;
 *   单 IP 的特征(如小样本成功率偏高)重复命中若按次叠加,
 *   正常用户也会在几十秒内被推到 60 分封禁线,压测时曾因此误封 600+ 个公网 IP。
 *   按规则种类计分后,单一弱特征最高只有该规则分(20~30),不会越过告警线。
 *
 * 为什么封禁指令要单独侧输出、不受冷却约束:
 *   告警面向"人",需要去重降噪;封禁面向"系统",必须即时执行。
 *   若复用到告警链路,主体跨过 40 分就已告警并进入冷却,
 *   冷却期内分数继续升高到 60+ 也不会再产生告警事件,封禁永远不会触发。
 */
public class RiskScoreProcessFunction extends KeyedProcessFunction<String, DetectionEvent, AlertEvent> {

    /** 侧输出:所有主体最新风险分(供 Redis 风险榜展示,含未达告警线主体) */
    public static final OutputTag<String> SCORE_TAG =
            new OutputTag<String>("risk-score") {};
    /** 侧输出:达到自动封禁线的 IP 主体(subject|score),不受告警冷却限制 */
    public static final OutputTag<String> BAN_TAG =
            new OutputTag<String>("auto-ban") {};

    /** 规则命中的有效期:超过该时长未再命中的规则从评分中移除,避免历史高分长期滞留 */
    private static final long SCORE_RESET_MS = 5 * 60 * 1000L;
    /** 自动封禁至少需要的规则种类数:单规则命中的误报不足以直接封禁 */
    private static final int BAN_MIN_RULES = 2;

    private final JobConfig cfg;

    /** 最近一次告警的事件时间(冷却依据) */
    private transient ValueState<Long> lastAlertTs;
    /** 规则命中明细:"规则码:时间戳:分值",多个用逗号分隔 */
    private transient ValueState<String> ruleHits;

    public RiskScoreProcessFunction(JobConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public void open(Configuration parameters) {
        lastAlertTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastAlertTs", Long.class));
        ruleHits = getRuntimeContext().getState(
                new ValueStateDescriptor<>("ruleHits", String.class));
    }

    @Override
    public void processElement(DetectionEvent d, Context ctx, Collector<AlertEvent> out) throws Exception {
        long eventTs = d.windowEndMs;

        // 1. 取出现存规则命中,丢弃超过有效期的
        Map<String, long[]> hits = new LinkedHashMap<>();
        String raw = ruleHits.value();
        if (raw != null && !raw.isEmpty()) {
            for (String item : raw.split(",")) {
                String[] p = item.split(":");
                if (p.length != 3) continue;
                long ts = Long.parseLong(p[1]);
                if (eventTs - ts <= SCORE_RESET_MS) {
                    hits.put(p[0], new long[]{ts, Long.parseLong(p[2])});
                }
            }
        }

        // 2. 记录本次命中:同规则只刷新时间与分值,不累加
        hits.put(d.ruleCode, new long[]{eventTs, d.score});

        // 3. 评分 = 各类规则命中分之和,封顶 100
        int newScore = 0;
        for (long[] v : hits.values()) newScore += (int) v[1];
        newScore = Math.min(100, newScore);

        // 4. 回写状态
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, long[]> e : hits.entrySet()) {
            if (sb.length() > 0) sb.append(',');
            sb.append(e.getKey()).append(':').append(e.getValue()[0]).append(':').append(e.getValue()[1]);
        }
        ruleHits.update(sb.toString());

        // 实时更新风险榜(侧输出 JSON:subject|score|level)
        ctx.output(SCORE_TAG, d.subjectType + "|" + d.subject + "|" + newScore + "|" + AlertEvent.levelOf(newScore));

        // 自动封禁:IP 主体评分达线且至少命中 2 类规则(与告警冷却解耦)
        if ("ip".equals(d.subjectType) && newScore >= cfg.autoBanScore && hits.size() >= BAN_MIN_RULES) {
            ctx.output(BAN_TAG, d.subject + "|" + newScore);
        }

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
