package com.flink.market_analysis;

import com.flink.market_analysis.beans.ChannelPromotionCount;
import com.flink.market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName AppMarketingByChannel
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1.从自定义数据源中读取数据
        DataStream<MarketingUserBehavior> dataStream = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                });

        //2.分渠道开窗数据
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream.filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy("channel", "behavior")    //定义组合键
                .timeWindow(Time.hours(1), Time.seconds(5))  //定义滑窗
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        resultStream.print("app marketing by channel job");

        env.execute();

    }

    //实现自定义的增量聚合函数
    public static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    //实现自定义的全窗口函数
    public static class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {

        @Override
        public void process(Tuple tuple, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            Long count = elements.iterator().next();

            out.collect(new ChannelPromotionCount(channel, behavior, windowEnd, count));
        }
    }


    //实现自定义的模拟市场用户行为数据源
    public static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {
        // 控制是否正常运行的标识位
        Boolean running = true;

        //定义用户行为和渠道范围
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> chanelList = Arrays.asList("app store", "wechat", "weibo");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (running) {
                //随机生成所有字段
                Long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = chanelList.get(random.nextInt(chanelList.size()));
                Long timeStamp = System.currentTimeMillis();

                //发出数据
                ctx.collect(new MarketingUserBehavior(id, behavior, channel, timeStamp));

                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
