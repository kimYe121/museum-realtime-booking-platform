package com.museum.function;

import com.museum.config.JobConfig;
import com.museum.model.DetectionEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 异常流量检测规则(R1~R6)。
 * 仅使用业务字段(ip / session_id / ticket_count / result),
 * 绝不使用 traffic_type、is_abnormal 标注真值。
 *
 * 状态优化:全部基于 MetricAggregate 的聚合累加器(acc[0]=count,
 * acc[1]=ticketSum, acc[2]=successCount)判定,窗口状态只存 3 个 long,
 * 不缓存原始事件,内存开销与事件量无关,支撑 5000+ 条/秒持续运行。
 */
public class DetectFunctions {

    /** R1 单 IP 高频:10s 滑动窗口内同一 IP 请求数超阈值 */
    public static class IpFreqDetect extends ProcessWindowFunction<long[], DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public IpFreqDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String ip, Context ctx, Iterable<long[]> acc, Collector<DetectionEvent> out) {
            long count = acc.iterator().next()[0];
            if (count >= cfg.ipFreqThreshold) {
                out.collect(new DetectionEvent("R1", "单IP高频请求", "ip", ip,
                        count, cfg.ipFreqThreshold, 30, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }

    /** R2 单 IP 高票数:60s 窗口内同一 IP 累计预约票数异常 */
    public static class IpTicketDetect extends ProcessWindowFunction<long[], DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public IpTicketDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String ip, Context ctx, Iterable<long[]> acc, Collector<DetectionEvent> out) {
            long tickets = acc.iterator().next()[1];
            if (tickets >= cfg.ipTicketThreshold) {
                out.collect(new DetectionEvent("R2", "单IP累计高票数", "ip", ip,
                        tickets, cfg.ipTicketThreshold, 25, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }

    /** R3 成功率异常:IP 成功率显著高于全局基线(约 0.86)且请求量足够 */
    public static class SuccRateDetect extends ProcessWindowFunction<long[], DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public SuccRateDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String ip, Context ctx, Iterable<long[]> acc, Collector<DetectionEvent> out) {
            long[] a = acc.iterator().next();
            if (a[0] >= cfg.succMinReq) {
                double rate = (double) a[2] / a[0];
                if (rate >= cfg.succRateThreshold) {
                    out.collect(new DetectionEvent("R3", "成功率显著偏高", "ip", ip,
                            rate, cfg.succRateThreshold, 25, ctx.window().getStart(), ctx.window().getEnd()));
                }
            }
        }
    }

    /** R4 固定会话高频:10s 窗口内同一 session_id 请求数超阈值 */
    public static class SessionFreqDetect extends ProcessWindowFunction<long[], DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public SessionFreqDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String sessionId, Context ctx, Iterable<long[]> acc, Collector<DetectionEvent> out) {
            long count = acc.iterator().next()[0];
            if (count >= cfg.sessionFreqThreshold) {
                out.collect(new DetectionEvent("R4", "固定会话高频请求", "session", sessionId,
                        count, cfg.sessionFreqThreshold, 20, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }

    /** R5 黄牛网段突增:10s 窗口内同一 /24 网段请求量超阈值 */
    public static class SubnetSurgeDetect extends ProcessWindowFunction<long[], DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public SubnetSurgeDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String subnet, Context ctx, Iterable<long[]> acc, Collector<DetectionEvent> out) {
            long count = acc.iterator().next()[0];
            if (count >= cfg.subnetFreqThreshold) {
                out.collect(new DetectionEvent("R5", "网段请求量突增", "subnet", subnet + ".0/24",
                        count, cfg.subnetFreqThreshold, 20, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }

    /**
     * R6 私网/机房网段聚集:30s 窗口内来自同一私网 /16 的请求超阈值。
     * 依据:博物馆公网预约服务的正常用户不可能来自 10.x/172.16~31.x/192.168.x
     * 等私网地址段,该特征与黄牛租用的机房代理多账号行为一致。
     */
    public static class PrivateNetDetect extends ProcessWindowFunction<long[], DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public PrivateNetDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String subnet16, Context ctx, Iterable<long[]> acc, Collector<DetectionEvent> out) {
            long count = acc.iterator().next()[0];
            if (count >= cfg.privateNetThreshold) {
                out.collect(new DetectionEvent("R6", "私网网段聚集访问", "subnet16", subnet16 + ".0.0/16",
                        count, cfg.privateNetThreshold, 20, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }
}
