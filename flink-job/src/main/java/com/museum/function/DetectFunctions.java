package com.museum.function;

import com.museum.config.JobConfig;
import com.museum.model.BookingEvent;
import com.museum.model.DetectionEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 异常流量检测规则(R1~R5)。
 * 仅使用业务字段(ip / session_id / ticket_count / result),
 * 绝不使用 traffic_type、is_abnormal 标注真值。
 */
public class DetectFunctions {

    /** R1 单 IP 高频:10s 滑动窗口内同一 IP 请求数超阈值 */
    public static class IpFreqDetect extends ProcessWindowFunction<BookingEvent, DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public IpFreqDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String ip, Context ctx, Iterable<BookingEvent> elements, Collector<DetectionEvent> out) {
            int count = 0;
            for (BookingEvent ignored : elements) count++;
            if (count >= cfg.ipFreqThreshold) {
                out.collect(new DetectionEvent("R1", "单IP高频请求", "ip", ip,
                        count, cfg.ipFreqThreshold, 30, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }

    /** R2 单 IP 高票数:60s 窗口内同一 IP 累计预约票数异常 */
    public static class IpTicketDetect extends ProcessWindowFunction<BookingEvent, DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public IpTicketDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String ip, Context ctx, Iterable<BookingEvent> elements, Collector<DetectionEvent> out) {
            long tickets = 0;
            for (BookingEvent e : elements) tickets += e.ticketCount;
            if (tickets >= cfg.ipTicketThreshold) {
                out.collect(new DetectionEvent("R2", "单IP累计高票数", "ip", ip,
                        tickets, cfg.ipTicketThreshold, 25, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }

    /** R3 成功率异常:IP 成功率显著高于全局基线(约 0.86)且请求量足够 */
    public static class SuccRateDetect extends ProcessWindowFunction<BookingEvent, DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public SuccRateDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String ip, Context ctx, Iterable<BookingEvent> elements, Collector<DetectionEvent> out) {
            int count = 0, success = 0;
            for (BookingEvent e : elements) {
                count++;
                if (e.isSuccess()) success++;
            }
            if (count >= cfg.succMinReq) {
                double rate = (double) success / count;
                if (rate >= cfg.succRateThreshold) {
                    out.collect(new DetectionEvent("R3", "成功率显著偏高", "ip", ip,
                            rate, cfg.succRateThreshold, 25, ctx.window().getStart(), ctx.window().getEnd()));
                }
            }
        }
    }

    /** R4 固定会话高频:10s 窗口内同一 session_id 请求数超阈值 */
    public static class SessionFreqDetect extends ProcessWindowFunction<BookingEvent, DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public SessionFreqDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String sessionId, Context ctx, Iterable<BookingEvent> elements, Collector<DetectionEvent> out) {
            int count = 0;
            for (BookingEvent ignored : elements) count++;
            if (count >= cfg.sessionFreqThreshold) {
                out.collect(new DetectionEvent("R4", "固定会话高频请求", "session", sessionId,
                        count, cfg.sessionFreqThreshold, 20, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }

    /** R5 黄牛网段突增:10s 窗口内同一 /24 网段请求量超阈值 */
    public static class SubnetSurgeDetect extends ProcessWindowFunction<BookingEvent, DetectionEvent, String, TimeWindow> {
        private final JobConfig cfg;
        public SubnetSurgeDetect(JobConfig cfg) { this.cfg = cfg; }

        @Override
        public void process(String subnet, Context ctx, Iterable<BookingEvent> elements, Collector<DetectionEvent> out) {
            int count = 0;
            for (BookingEvent ignored : elements) count++;
            if (count >= cfg.subnetFreqThreshold) {
                out.collect(new DetectionEvent("R5", "网段请求量突增", "subnet", subnet + ".0/24",
                        count, cfg.subnetFreqThreshold, 20, ctx.window().getStart(), ctx.window().getEnd()));
            }
        }
    }
}
