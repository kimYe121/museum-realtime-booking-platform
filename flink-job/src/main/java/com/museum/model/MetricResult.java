package com.museum.model;

/**
 * 窗口聚合结果:某维度键在一个窗口内的统计值。
 */
public class MetricResult {
    public String metric;      // 指标类型: qps/venue/slot/client/result/ticket_dist/venueslot
    public String dimKey;      // 维度键(馆区、时段、客户端等,qps 时为 all)
    public long windowEndMs;   // 窗口结束时间(事件时间)
    public long count;         // 请求数
    public long ticketSum;     // 票数合计
    public long successCount;  // 成功数

    public MetricResult() {}

    public MetricResult(String metric, String dimKey, long windowEndMs,
                        long count, long ticketSum, long successCount) {
        this.metric = metric;
        this.dimKey = dimKey;
        this.windowEndMs = windowEndMs;
        this.count = count;
        this.ticketSum = ticketSum;
        this.successCount = successCount;
    }

    public double successRate() {
        return count == 0 ? 0.0 : (double) successCount / count;
    }

    public String toJson() {
        return String.format(
                "{\"windowEnd\":%d,\"count\":%d,\"ticketSum\":%d,\"successCount\":%d,\"successRate\":%.4f}",
                windowEndMs, count, ticketSum, successCount, successRate());
    }
}
