package com.flink.order_detect;

import com.flink.order_detect.beans.OrderEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;


/**
 * @author yanshupan
 * @version 1.0
 * @ClassName OrderPayTimeOut
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class OrderPayTimeOut {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);



        //读取数据并转换成POJO类型
        URL resource = OrderPayTimeOut.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventDataStream = env.readTextFile(resource.getPath())
                .map( line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]),fields[1],fields[2],new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimeStamp() * 1000;
                    }
                });

        //1.定义一个带时间限制的模式

        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {

            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.geteventType());
            }
        })
                .followedBy("pay").where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.geteventType());
                    }
                })
                .within(Time.minutes(15));




    }
}
