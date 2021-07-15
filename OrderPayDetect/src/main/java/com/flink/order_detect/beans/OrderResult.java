package com.flink.order_detect.beans;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName OrderResult
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class OrderResult {
    private Long id;
    private String resultState;

    public OrderResult() {
    }

    public OrderResult(Long id, String resultState) {
        this.id = id;
        this.resultState = resultState;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getResultState() {
        return resultState;
    }

    public void setResultState(String resultState) {
        this.resultState = resultState;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "id=" + id +
                ", resultState='" + resultState + '\'' +
                '}';
    }
}

