package com.flink.network.function;

import com.flink.network.beans.PageViewCount;
import com.flink.network.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;
import java.util.Random;

/**
 * @author yanshupan
 * @version 1.0
 * @ClassName UvWithBloomFilter
 * @DESCRIPTION TODO
 * @Date
 * @since 1.0
 */
public class UvWithBloomFilter {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        SingleOutputStreamOperator<PageViewCount> UvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFliter());

        UvStream.print();

        env.execute("uv count job");
    }

    //自定义触发器
    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            //每一条数据来到，直接触发窗口计算，并且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    //自定义一个布隆过滤器
    public static class MyBloomFilter {
        //定义位图的大小，一般需要定义为2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        //实现一个hash函数
        public Long hashCode(String value, Integer seed) {
            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }

    //实现自定义的处理函数
    public static class UvCountResultWithBloomFliter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        //定义一个jedis连接和布隆过滤器
        Jedis jedis;
        MyBloomFilter myBloomFilter;


        @Override
        public void open(Configuration parameters) throws Exception {
            //定义jedis连接配置
        /*
        * redis在启动的时候默认会启动一个保护模式，只有同一个服务器可以连接上redis。别的服务器连接不上这个redis
          解决办法：关闭保护模式
        1、进入redis安装目录找到conf目录下的redis.conf文件
            vi redis.conf 注释bind 127.0.0.1这一行
        2、启动redis 登入客户端  执行下面命令
            config set protected-mode "no"
        * */
            jedis = new Jedis("node03", 6379);
            myBloomFilter = new MyBloomFilter(1 << 29); //可以写2^29,也可以位移的方式表示，这个意思是1后面29个0，因为要处理1亿的数据，用64MB大小位图
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
            // 将位图和窗口count值，全部存入redis，用windowEnd作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();

            // 把count值存成一张hash表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            // 1. 取当前的userID
            Long userId = elements.iterator().next().getUserId();

            //2.计算位图中的offset
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            //3.用redis的getbit命令，判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey, offset);

            if (!isExist) {
                // 如果不存在，对应位图的位置要置1
                jedis.setbit(bitmapKey, offset, true);

                //更新redis中保存的count值
                long uvCount = 0L;//初始化count值
                String uvCountString = jedis.hget(countHashName, countKey);
                if (uvCountString != null && !"".equals(uvCountString)) {
                    uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(countHashName, countKey, String.valueOf(uvCount + 1));

                out.collect(new PageViewCount("UV", windowEnd, uvCount + 1));
            }
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }


}
