package com.ljj.toshow.demos.bean;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ljj.toshow.ToShowApplication;
import com.ljj.toshow.demos.pojo.TrafficEventUtils;
import com.ljj.toshow.demos.pojo.poiS;
import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Component
public class StartupRunner implements CommandLineRunner {

    public static RedisTemplate<String, poiS> redisTemplate;
    @Getter
    private final TrafficEventUtils.MileageConverter mileageConverter1;
    @Getter
    private final TrafficEventUtils.MileageConverter mileageConverter2;

    private  String brokers;
    private List<String> topics; // 改为非 final，允许动态注入

    @Autowired
    public StartupRunner(
        RedisTemplate<String, poiS> redisTemplate,
        @Qualifier("mileageConverter1") TrafficEventUtils.MileageConverter converter1,
        @Qualifier("mileageConverter2") TrafficEventUtils.MileageConverter converter2,
        @Value("${kafka.brokers}") String brokers
    ) {
        this.redisTemplate = redisTemplate;
        this.mileageConverter1 = converter1;
        this.mileageConverter2 = converter2;
    }

    @Override
    public void run(String... args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: java -jar your-app.jar <broker> <topic1> [topic2...]");
        }
        this.brokers =  args[0];
        this.topics = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));
        startFlinkJob();
    }


    private void startFlinkJob() throws Exception {
         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//         Kafka配置
//        String brokers = "100.65.38.40:9092";
        String groupId = "flink-group";

//List<String> topics = Arrays.asList("fiberDataTest1", "fiberDataTest2", "fiberDataTest3");
        // 构建Kafka数据流
        DataStream<String> unionStream = buildKafkaStream(env, brokers, groupId, topics);
         // 处理数据流时使用静态内部类并传递转换器
    DataStream<poiS> pointStream = unionStream
        .flatMap(new PoiFlatMap(mileageConverter1, mileageConverter2));

        // 添加内存存储Sink
        pointStream.addSink(new RedisSink());

        env.execute("Real-time Vehicle Points Processing");
    }

     // Kafka流构建方法
    public static DataStream<String> buildKafkaStream(StreamExecutionEnvironment env,
                                                      String brokers, String groupId,
                                                      List<String> topics) {
      KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(brokers)
            .setTopics(topics) // 直接使用列表
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

// 新增静态内部类实现FlatMapFunction
private static class PoiFlatMap implements FlatMapFunction<String, poiS>, Serializable {
    private final TrafficEventUtils.MileageConverter mileageConverter1;
    private final TrafficEventUtils.MileageConverter mileageConverter2;

    public PoiFlatMap(TrafficEventUtils.MileageConverter converter1,
                      TrafficEventUtils.MileageConverter converter2) {
        this.mileageConverter1 = converter1;
        this.mileageConverter2 = converter2;
    }

    @Override
    public void flatMap(String jsonString, Collector<poiS> out) {
        try {
            JSONObject jsonObject = JSON.parseObject(jsonString);
            for (JSONObject JSONpoint : JSON.parseArray(jsonObject.getString("pathList"), JSONObject.class)) {
                preprocessPoint(JSONpoint);
//                System.out.println("json: "+JSONpoint.toJSONString());
                poiS poi = convertToPoiS(JSONpoint);
//                System.out.println("poi: "+poi);
                out.collect(poi);
            }
        } catch (Exception e) {
            System.err.println("处理数据时出错: " + e.getMessage());
            }
    }

    private void preprocessPoint(JSONObject point) {
        if (!point.containsKey("specialFlag")) {
            point.put("specialFlag", "0");
        }
    }

    private poiS convertToPoiS(JSONObject point) {
        poiS p = new poiS();
        p.setStakeId(point.getString("stakeId"));
        p.setName(point.getString("plateNo"));
        p.setDirection(point.getInteger("direction"));
//        System.out.println("p1:"+p);
        if(point.getDouble("latitude")==null||point.getDouble("longitude")==null){
             TrafficEventUtils.MileageConverter converter = (p.getDirection() == 1)
                ? mileageConverter1 : mileageConverter2;
             double[] lnglat = converter.findCoordinate(point.getInteger("mileage")).getLnglat();
             p.setLatitude(lnglat[1]);
             p.setLongitude(lnglat[0]);
        }else{
            p.setLatitude(point.getDouble("latitude"));
            p.setLongitude(point.getDouble("longitude"));
        }

//        System.out.println("p2:"+p);

//                p.setLatitude(point.getDouble("latitude"));
//        p.setLongitude(point.getDouble("longitude"));
        return p;
    }
}
    public static class RedisSink extends RichSinkFunction<poiS> {


    @Override
    public void open(Configuration parameters) {
        LettuceConnectionFactory factory = new LettuceConnectionFactory("100.65.38.139", 6379);
        factory.afterPropertiesSet();

        redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(factory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        // 使用 JSON 序列化器
        redisTemplate.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        redisTemplate.afterPropertiesSet();
    }

    @Override
    public void invoke(poiS value, Context context) {
        // 直接存储对象，序列化器会自动转换为 JSON
        redisTemplate.opsForValue().set(
            "v" + value.getName(),
            value,
            30, TimeUnit.SECONDS
        );
//        System.out.println("成功写入 Redis: " + value.getName());
    }
}
//    // 内存存储Sink实现
//    public static class InMemoryListSink extends RichSinkFunction<poiS> {
//
//        @Override
//        public void open(Configuration parameters) {
//
//            // 初始化操作（如有需要）
//        }
//
//        @Override
//        public void invoke(poiS value, Context context) {
////            System.out.println("start invoke,value:" + value);
//            resMap.put(value.getName(), value);
//            // 调试输出
//            if (resMap.size() % 1000 == 0) {
//                System.out.println("当前存储点数: " + resMap.size());
//            }
//        }
//
//
//    }
    public static int stakeToMileage(String stakeId) {
            return Integer.parseInt(stakeId.split("\\+")[0].substring(1)) * 1000 + Integer.parseInt(stakeId.split("\\+")[1]);
        }
}
