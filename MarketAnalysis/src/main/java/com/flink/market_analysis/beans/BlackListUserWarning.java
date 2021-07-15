package com.flink.market_analysis.beans;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName BlackListUserWarning
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class BlackListUserWarning {
    private Long userId;
    private Long adId;
    private String warning;

    public BlackListUserWarning() {
    }

    public BlackListUserWarning(Long userId, Long adId, String warning) {
        this.userId = userId;
        this.adId = adId;
        this.warning = warning;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getWarning() {
        return warning;
    }

    public void setWarning(String warning) {
        this.warning = warning;
    }

    @Override
    public String toString() {
        return "BlackListUserWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", warning='" + warning + '\'' +
                '}';
    }
}
