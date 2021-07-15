package com.flink.network.beans;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName com.flink.network.beans.ApacheLogEvent
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class ApacheLogEvent {
        private String id;
        private String log;
        private Long timestamp;
        private String method;
        private String url;

    public ApacheLogEvent() {
    }

    public ApacheLogEvent(String id, String log, Long timestamp, String method, String url) {
        this.id = id;
        this.log = log;
        this.timestamp = timestamp;
        this.method = method;
        this.url = url;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMethod() {
        return method;
    }


    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "com.flink.network.beans.ApacheLogEvent{" +
                "id='" + id + '\'' +
                ", log='" + log + '\'' +
                ", timestamp=" + timestamp +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
