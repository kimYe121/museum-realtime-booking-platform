package com.museum.function;

import com.museum.model.BookingEvent;
import com.museum.model.MetricResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 通用窗口聚合:count / ticketSum / successCount。
 * 配合 keyBy 后的各类维度窗口使用。
 */
public class MetricAggregate implements AggregateFunction<BookingEvent, long[], long[]> {

    @Override
    public long[] createAccumulator() {
        return new long[3]; // [count, ticketSum, successCount]
    }

    @Override
    public long[] add(BookingEvent e, long[] acc) {
        acc[0]++;
        acc[1] += e.ticketCount;
        if (e.isSuccess()) acc[2]++;
        return acc;
    }

    @Override
    public long[] getResult(long[] acc) {
        return acc;
    }

    @Override
    public long[] merge(long[] a, long[] b) {
        a[0] += b[0];
        a[1] += b[1];
        a[2] += b[2];
        return a;
    }

    /**
     * 把聚合结果包装成 MetricResult(key 来自 keyBy)。
     */
    public static class Wrap extends ProcessWindowFunction<long[], MetricResult, String, TimeWindow> {
        private final String metric;

        public Wrap(String metric) {
            this.metric = metric;
        }

        @Override
        public void process(String key, Context ctx, Iterable<long[]> acc, Collector<MetricResult> out) {
            long[] a = acc.iterator().next();
            out.collect(new MetricResult(metric, key, ctx.window().getEnd(), a[0], a[1], a[2]));
        }
    }
}
