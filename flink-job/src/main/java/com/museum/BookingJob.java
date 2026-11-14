package com.museum;

import com.museum.config.JobConfig;
import com.museum.function.DetectFunctions;
import com.museum.function.MetricAggregate;
import com.museum.function.ParseValidateFunction;
import com.museum.function.PressureProcessFunction;
import com.museum.function.RiskScoreProcessFunction;
import com.museum.model.AlertEvent;
import com.museum.model.BookingEvent;
import com.museum.model.DetectionEvent;
import com.museum.model.MetricResult;
import com.museum.sink.JdbcSinks;
import com.museum.sink.RedisSinks;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 博物馆票务预约实时计算主作业。
 *
 * 拓扑:
 *   Kafka(raw) → 解析校验(非法→错误 topic / 黑名单拦截)
 *              → EventTime + Watermark
 *              → 实时指标(7 类窗口聚合 → Redis,部分 → MySQL)
 *              → 余票压力(keyed state 累计 → Redis)
 *              → 异常检测 R1~R5 → 风险评分+冷却 → 告警(Redis + MySQL)
 */
public class BookingJob {

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.fromEnv();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ============ 1. 数据接入 ============
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.kafkaBrokers)
                .setTopics(cfg.topicRaw)
                .setGroupId(cfg.consumerGroup)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-raw-source");

        // ============ 2. 清洗:解析校验 + 黑名单拦截 ============
        SingleOutputStreamOperator<BookingEvent> valid = raw
                .process(new ParseValidateFunction(cfg))
                .name("parse-validate");

        // 非法数据 → 错误 topic + Redis 计数
        DataStream<String> invalid = valid.getSideOutput(ParseValidateFunction.INVALID_TAG);
        KafkaSink<String> errorSink = KafkaSink.<String>builder()
                .setBootstrapServers(cfg.kafkaBrokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(cfg.topicError)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        invalid.sinkTo(errorSink).name("kafka-error-sink");
        invalid.addSink(new RedisSinks.Counter(cfg, "stats:invalid")).name("invalid-count");

        // 黑名单命中 → Redis 计数(拦截量统计)
        valid.getSideOutput(ParseValidateFunction.BLACKLIST_TAG)
                .addSink(new RedisSinks.Counter(cfg, "stats:blacklist_hits"))
                .name("blacklist-hit-count");

        // ============ 3. EventTime + Watermark ============
        DataStream<BookingEvent> timed = valid.assignTimestampsAndWatermarks(
                WatermarkStrategy.<BookingEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(cfg.outOfOrderSec))
                        .withTimestampAssigner((e, ts) -> e.eventTsMs)
                        .withIdleness(Duration.ofSeconds(30)));

        // ============ 4. 实时指标(窗口聚合 → Redis / MySQL) ============
        MetricAggregate agg = new MetricAggregate();

        // 4.1 全站 1 分钟 QPS(滚动窗口)
        SingleOutputStreamOperator<MetricResult> qps = timed
                .keyBy(e -> "all")
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(agg, new MetricAggregate.Wrap("qps_1m"));
        qps.addSink(new RedisSinks.Qps(cfg)).name("redis-qps");
        qps.addSink(new JdbcSinks.WindowStats(cfg)).name("mysql-qps-snapshot");

        // 4.1b 秒级实时流量(10 秒滚动窗口,平滑实时曲线)
        timed.keyBy(e -> "all")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(agg, new MetricAggregate.Wrap("qps10s"))
                .addSink(new RedisSinks.Trend10s(cfg)).name("redis-trend-10s");

        // 4.2 分馆区(5 分钟滚动窗口) → Redis + MySQL(准确性核对)
        SingleOutputStreamOperator<MetricResult> venue = timed
                .keyBy(e -> e.venueName)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(agg, new MetricAggregate.Wrap("venue_5m"));
        venue.addSink(new RedisSinks.MetricHash(cfg)).name("redis-venue");
        venue.addSink(new JdbcSinks.WindowStats(cfg)).name("mysql-venue-snapshot");

        // 4.3 分时段
        timed.keyBy(e -> e.timeSlotName)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(agg, new MetricAggregate.Wrap("slot"))
                .addSink(new RedisSinks.MetricHash(cfg)).name("redis-slot");

        // 4.4 客户端类型分布
        timed.keyBy(e -> e.clientType)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(agg, new MetricAggregate.Wrap("client"))
                .addSink(new RedisSinks.MetricHash(cfg)).name("redis-client");

        // 4.5 预约结果分布
        timed.keyBy(e -> e.result)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(agg, new MetricAggregate.Wrap("result"))
                .addSink(new RedisSinks.MetricHash(cfg)).name("redis-result");

        // 4.6 单次预约票数分布
        timed.keyBy(e -> String.valueOf(e.ticketCount))
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(agg, new MetricAggregate.Wrap("ticket_dist"))
                .addSink(new RedisSinks.MetricHash(cfg)).name("redis-ticket-dist");

        // 4.7 馆区x时段预约量(热门时段 TOP 排行数据源)
        timed.keyBy(BookingEvent::venueSlotName)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(agg, new MetricAggregate.Wrap("venueslot"))
                .addSink(new RedisSinks.MetricHash(cfg)).name("redis-venueslot");

        // 4.8 余票压力(keyed state 累计)
        timed.keyBy(BookingEvent::venueSlot)
                .process(new PressureProcessFunction(cfg))
                .name("pressure")
                .addSink(new RedisSinks.Pressure(cfg)).name("redis-pressure");

        // ============ 5. 异常流量检测 R1~R5 ============
        DataStream<DetectionEvent> r1 = timed
                .keyBy(e -> e.ip)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .process(new DetectFunctions.IpFreqDetect(cfg));

        DataStream<DetectionEvent> r2 = timed
                .keyBy(e -> e.ip)
                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5)))
                .process(new DetectFunctions.IpTicketDetect(cfg));

        DataStream<DetectionEvent> r3 = timed
                .keyBy(e -> e.ip)
                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5)))
                .process(new DetectFunctions.SuccRateDetect(cfg));

        DataStream<DetectionEvent> r4 = timed
                .keyBy(e -> e.sessionId)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .process(new DetectFunctions.SessionFreqDetect(cfg));

        DataStream<DetectionEvent> r5 = timed
                .keyBy(BookingEvent::subnet24)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .process(new DetectFunctions.SubnetSurgeDetect(cfg));

        DataStream<DetectionEvent> detections = r1.union(r2, r3, r4, r5);

        // ============ 6. 风险评分 + 告警(冷却去重) ============
        SingleOutputStreamOperator<AlertEvent> alerts = detections
                .keyBy(d -> d.subjectType + "|" + d.subject)
                .process(new RiskScoreProcessFunction(cfg))
                .name("risk-score");

        alerts.addSink(new RedisSinks.Alerts(cfg)).name("redis-alerts");
        alerts.addSink(new JdbcSinks.Alerts(cfg)).name("mysql-alerts");

        // 风险榜(含未达告警线主体)
        alerts.getSideOutput(RiskScoreProcessFunction.SCORE_TAG)
                .addSink(new RedisSinks.RiskBoard(cfg)).name("redis-risk-board");

        env.execute("museum-booking-realtime-job");
    }
}
