package com.museum.function;

import com.museum.config.JobConfig;
import com.museum.model.BookingEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 余票压力(进阶指标):
 * 按 馆区x时段 累计已预约票数(仅统计 SUCCESS),与假设总库存比较得到压力比。
 * 库存由配置 SLOT_CAPACITY 指定(每个馆区x时段一份)。
 */
public class PressureProcessFunction extends KeyedProcessFunction<String, BookingEvent, String> {

    private final JobConfig cfg;
    private transient ValueState<Long> bookedTickets;

    public PressureProcessFunction(JobConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public void open(Configuration parameters) {
        bookedTickets = getRuntimeContext().getState(
                new ValueStateDescriptor<>("bookedTickets", Long.class));
    }

    @Override
    public void processElement(BookingEvent e, Context ctx, Collector<String> out) throws Exception {
        if (!e.isSuccess()) return;
        Long booked = bookedTickets.value();
        long newBooked = (booked == null ? 0 : booked) + e.ticketCount;
        bookedTickets.update(newBooked);

        double ratio = Math.min(1.0, (double) newBooked / cfg.slotCapacity);
        out.collect(String.format(
                "{\"key\":\"%s\",\"display\":\"%s\",\"booked\":%d,\"capacity\":%d,\"ratio\":%.4f}",
                e.venueSlot(), e.venueSlotName(), newBooked, cfg.slotCapacity, ratio));
    }
}
