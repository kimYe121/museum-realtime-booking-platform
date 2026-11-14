package com.museum.model;

/**
 * 博物馆预约请求事件(与生成器 JSON 字段一一对应)。
 * 注意:traffic_type / is_abnormal 为标注真值,仅用于离线验证,
 * 计算链路中不参与任何统计与展示。
 */
public class BookingEvent {
    public String requestId;
    public String eventTime;
    public long eventTsMs;
    public String ip;
    public String clientType;
    public String venue;
    public String venueName;
    public String timeSlot;
    public String timeSlotName;
    public int ticketCount;
    public String result;
    public String sessionId;

    /** 清洗时标注:blacklisted 表示命中黑名单被拦截 */
    public transient boolean blacklisted;

    public boolean isSuccess() {
        return "SUCCESS".equals(result);
    }

    /** 所属 /24 网段 */
    public String subnet24() {
        if (ip == null) return "unknown";
        int i = ip.lastIndexOf('.');
        return i > 0 ? ip.substring(0, i) : ip;
    }

    /** 馆区x时段组合键 */
    public String venueSlot() {
        return venue + "|" + timeSlot;
    }

    public String venueSlotName() {
        return venueName + "-" + timeSlotName;
    }
}
