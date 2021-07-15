package com.flink.loginfail_detect;

import com.flink.loginfail_detect.beans.LoginEvent;
import com.flink.loginfail_detect.beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName LoginFail
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class LoginFail {
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

        //  自定义处理函数检测连续登陆失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        warningStream.print();

        env.execute("Warning Jon");
    }

   //方法二
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
       // 定义属性，最大连续登陆失败次数
       private Integer maxFailTimes;

       public LoginFailDetectWarning(Integer maxFailTimes) {
           this.maxFailTimes = maxFailTimes;
       }

       //定义状态：保存2秒内所有的登录失败事件
       ListState<LoginEvent> loginFailEventListState;

       @Override
       public void open(Configuration parameters) throws Exception {
           loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
       }

       //以登录事件判断报警的触发条件，不在注册定时器
       @Override
       public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            //判断当前事件登录状态
           if("fail".equals(value.getLoginState())){
               //1.如果是登录失败，获取状态中之前的登录失败事件，继续判断是否已有失败事件
               Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
               if(iterator.hasNext()){
                   //1.1 如果已经有登录失败事件，继续判断时间戳是否在2秒之内
                   LoginEvent firstFailEvent = iterator.next();
                   if(value.getTimestamp() - firstFailEvent.getTimestamp() <= 2){
                       //1.1.1 如果在2秒之内，输出报警
                       out.collect(new LoginFailWarning(value.getUserId(),firstFailEvent.getTimestamp(),value.getTimestamp(),"login fail two times in 2s"));
                   }
                   // 不管报不报警，这次都已经处理完毕，直接更新状态
                   //loginFailEventListState.update((ListState<LoginEvent>) value);
                   loginFailEventListState.clear();
                   loginFailEventListState.add(value);
               }else{
                   //1.2 如果没有登录失败，直接将当前时间存入ListState
                   loginFailEventListState.add(value);
               }
          }else{
               //2.如果是登录成功，直接清空状态
               loginFailEventListState.clear();
           }
       }
   }






    //方法一
   /*
   //实现自定义keyedProcessFunction
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        // 定义属性，最大连续登陆失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        //定义状态：保存2秒内所有的登录失败事件
        ListState<LoginEvent> loginFailEventListState;
        //定义状态： 保存注册的定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }


        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            //判断当前登录事件类型
            if ("fail".equals(value.getLoginState())) {
                //1.如果是失败事件，添加到列表状态中
                loginFailEventListState.add(value);
                //如果没有定时器，注册一个2秒之后的定时器
                if (timerTsState.value() == null) {
                    long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            }else {
                    //2.如果登录成功，删除定时器，清空状态，重新开始
                    if (timerTsState.value() != null)
                        ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                    loginFailEventListState.clear();
                    timerTsState.clear();
                }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            //定时器触发，说明2秒内没有登录成功来，判断ListState中失败的个数
            ArrayList<LoginEvent> loginFailsEvents = Lists.newArrayList(loginFailEventListState.get().iterator());
            Integer failTimes = loginFailsEvents.size();

            if(failTimes >= maxFailTimes){
                // 如果超出设定的最大的失败次数，输出报警
                out.collect( new LoginFailWarning(ctx.getCurrentKey(),loginFailsEvents.get(0).getTimestamp(),
                        loginFailsEvents.get(failTimes-1).getTimestamp(),"login fail in 2s for" + failTimes + "times" ));
            }
            //清空状态
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }
    */
}
