package com.flink.network.function;

import com.flink.network.beans.PageViewCount;
import com.flink.network.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName UniqueVisitor
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据创建DataStream

        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        //3.装换成pojo, 分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] s = line.split(",");
                    return new UserBehavior(new Long(s[0]), new Long(s[1]), new Integer(s[2]), (s[3]), new Long(s[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //开窗统计UV
        SingleOutputStreamOperator<PageViewCount>  pageViewCount = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());

        pageViewCount.print();

        env.execute("uv count job");

    }
    //实现自定义全窗口函数
    public static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow>{

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            // 定义一个set结构，保存窗口中的所有数据
            HashSet<Long> uidSet = new HashSet<>();
            for(UserBehavior ub : values)
                uidSet.add(ub.getUserId());
            out.collect(new PageViewCount("uv", window.getEnd(), (long)uidSet.size()));
        }
    }
}
