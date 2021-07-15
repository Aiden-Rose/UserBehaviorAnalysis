package com.flink.loginfail_detect.beans;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName LoginFailWarning
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class LoginFailWarning {
    private Long userId;
    private Long firstFailTime;
    private Long lastFailTime;
    private String warningMsg;

    public LoginFailWarning() {
    }

    public LoginFailWarning(Long userId, Long firstFailTime, Long lastFailTime, String warningMsg) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailTime;
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "LoginFailWarning{" +
                "userId=" + userId +
                ", firstFailTime=" + firstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }
}
