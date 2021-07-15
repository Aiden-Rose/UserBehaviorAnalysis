package com.flink.loginfail_detect;

import com.flink.loginfail_detect.beans.LoginEvent;
import com.flink.loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName LoginFailWithCEP
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class LoginFailWithCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1. 从文件中读取数据
        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //1. 定义一个匹配模式
        //firstFail -> secondFail, within 2s
        Pattern<LoginEvent, LoginEvent> loginFailPattern0 = Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        })
                .next("secondFail").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                })
                .next("thirdFail").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                })
                .within(Time.seconds(3));

        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("failEvents").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).times(3).consecutive()   //连续，必须按照时间连续失败3次才会输出报警
                .within(Time.seconds(5));



        //2. 将匹配模式应用到数据流上，得到一个pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), loginFailPattern);

        //3. 检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new LoginFailMatchDetectWarning());

        warningStream.print();

        env.execute("Login Fail detect with cep job ");
    }

    //自定义实现 PatternSelectFunction

    public static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent,LoginFailWarning>{

        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
//            LoginEvent firstFailEvent = map.get("firstFail").iterator().next();
//            LoginEvent thirdFailEvent = map.get("thirdFail").iterator().next();
//            return new LoginFailWarning(firstFailEvent.getUserId(),firstFailEvent.getTimestamp(),thirdFailEvent.getTimestamp(),"login fail three times in 3s");
            LoginEvent firstFailEvent = map.get("failEvents").get(0);
            LoginEvent lastFailEvent = map.get("failEvents").get(map.get("failEvents").size() - 1);
            return  new LoginFailWarning(firstFailEvent.getUserId(),firstFailEvent.getTimestamp(),lastFailEvent.getTimestamp(),"login fail " + map.get("failEvents").size() + " times in 5s");
        }}


}
