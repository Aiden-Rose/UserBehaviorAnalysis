package com.flink.order_detect.beans;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName OrderEvent
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class OrderEvent {
    private Long userId;
    private String eventType;
    private String txId;
    private Long timeStamp;


    public OrderEvent() {
    }

    public OrderEvent(Long userId, String eventType, String txId, Long timeStamp) {
        this.userId = userId;
        this.eventType = eventType;
        this.txId = txId;
        this.timeStamp = timeStamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String geteventType() {
        return eventType;
    }

    public void seteventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId=" + userId +
                ", eventType='" + eventType + '\'' +
                ", txId='" + txId + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
