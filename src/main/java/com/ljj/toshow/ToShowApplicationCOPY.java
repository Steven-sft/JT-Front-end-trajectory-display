//package com.ljj.toshow;
//
//import com.alibaba.fastjson2.JSON;
//import com.alibaba.fastjson2.JSONObject;
//import com.ljj.toshow.demos.pojo.Location;
//import com.ljj.toshow.demos.pojo.TrafficEventUtils;
//import com.ljj.toshow.demos.pojo.poiS;
//import com.ljj.toshow.demos.pojo.stakeEnvents;
//import com.ljj.toshow.demos.tools.JsonReader;
//import javafx.util.Pair;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.util.Collector;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.CopyOnWriteArrayList;
//
//@SpringBootApplication
//public class ToShowApplication {
//private static TrafficEventUtils.MileageConverter stakeAssign;
//
////前端每200ms请求一次reslist。reslist每一次都是当前这帧的数据。会不会查询到上一次的数据呢？不会，看你的reslist怎么更新
//    // 使用线程安全的List实现
//    public static final ConcurrentHashMap<String,poiS> resMap=new ConcurrentHashMap<>();
//     public static TrafficEventUtils.MileageConverter mileageConverter1 ;
//     public static TrafficEventUtils.MileageConverter mileageConverter2 ;
//
//    public static void main(String[] args) throws Exception {
//           try {
//              mileageConverter1 = new TrafficEventUtils.MileageConverter(args[0]);
//              mileageConverter2 = new TrafficEventUtils.MileageConverter(args[1]);
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//
//        SpringApplication.run(ToShowApplication.class, args);
//        startFlinkJob();
//    }
//
//    private static void startFlinkJob() throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//
////         Kafka配置
//        String brokers = "100.65.38.40:9092";
//        String groupId = "flink-group";
//        List<String> topics = Arrays.asList("MergedPathData.sceneTest.1",
//                "MergedPathData.sceneTest.2", "MergedPathData.sceneTest.3",
//                "MergedPathData.sceneTest.4", "MergedPathData.sceneTest.5");
////List<String> topics = Arrays.asList("fiberDataTest1", "fiberDataTest2", "fiberDataTest3");
//        // 构建Kafka数据流
//        DataStream<String> unionStream = buildKafkaStream(env, brokers, groupId, topics);
////   配置Kafka连接信息
//
////            String brokers = "100.65.38.40:9092";
////            String groupId = "flink_consumer_group";
////            List<String> topics = Arrays.asList("MergedPathData");
//////            List<String> topics = Collections.singletonList("news-topic");
//////            List<String> topics = Collections.singletonList("MergedPathData.sceneTest.1");
//////             创建Kafka数据源
////            KafkaSource<String> source = KafkaSource.<String>builder()
////                    .setBootstrapServers(brokers)
////                    .setTopics(topics)
////                    .setGroupId(groupId)
////                    .setStartingOffsets(OffsetsInitializer.latest())
////                    .setValueOnlyDeserializer(new SimpleStringSchema())
////                    .setProperty("message.max.bytes", "16777216")
////                    .setProperty("max.partition.fetch.bytes", "16777216")
////                    .build();
////
////            // 从Kafka读取数据
////            DataStreamSource<String> unionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source1");
//
//        // 处理数据流
//        DataStream<poiS> pointStream = unionStream
//                .flatMap(new FlatMapFunction<String, poiS>() {
//
//                    @Override
//                    public void flatMap(String jsonString, Collector<poiS> out) {
//                        try {
//                            JSONObject jsonObject = JSON.parseObject(jsonString);
//                            for (JSONObject JSONpoint : JSON.parseArray(jsonObject.getString("pathList"), JSONObject.class)) {
//                                // 数据预处理
//                                preprocessPoint(JSONpoint);
//
//                                // 转换为poiS对象
//                                poiS point = convertToPoiS(JSONpoint);
//                                if (isValidPoint(point)) {
////                                    System.out.println(point);
//                                    out.collect(point);
//                                }
//                            }
//                        } catch (Exception e) {
//                            System.err.println("处理数据时出错: " + e.getMessage());
//                        }
//                    }
//
//                    private void preprocessPoint(JSONObject point) {
//                        // 确保specialFlag存在
//                        if (!point.containsKey("specialFlag")) {
//                            point.put("specialFlag", "0");
//                        }
//
//                        // 时间戳格式化处理
//                        String originalTime = point.getString("timeStamp");
//                        String[] parts = originalTime.split(":");
//                        if (parts.length == 4 && parts[3].length() == 2) {
//                            point.put("timeStamp",
//                                parts[0] + ":" + parts[1] + ":" + parts[2] + ":0" + parts[3]);
//                        }
//                    }
//
//                    private poiS convertToPoiS(JSONObject point) {
//                        return new poiS(
//                            point.getDouble("longitude"),
//                            point.getDouble("latitude"),
//                            point.getString("plateNo"),
//                            point.getString("skateID")
//                        );
//                    }
//
//                    private boolean isValidPoint(poiS point) {
//                        // 验证坐标范围（示例值，根据实际情况调整）
//                        return point.getLongitude() > 70 && point.getLongitude() < 140 &&
//                               point.getLatitude() > 0 && point.getLatitude() < 60;
//                    }
//                });
//
//
//        // 添加内存存储Sink
//        pointStream.addSink(new InMemoryListSink());
//
//        env.execute("Real-time Vehicle Points Processing");
//    }
//
//    // Kafka流构建方法
//    private static DataStream<String> buildKafkaStream(StreamExecutionEnvironment env,
//                                                      String brokers, String groupId,
//                                                      List<String> topics) {
//        DataStream<String> unionStream = null;
//        for (int i = 0; i < topics.size(); i++) {
//            KafkaSource<String> source = KafkaSource.<String>builder()
//                    .setBootstrapServers(brokers)
//                    .setTopics(topics.get(i))
//                    .setGroupId(groupId)
//                    .setStartingOffsets(OffsetsInitializer.latest())
//                    .setValueOnlyDeserializer(new SimpleStringSchema())
//                    .build();
//
//            DataStream<String> stream = env.fromSource(
//                source,
//                WatermarkStrategy.noWatermarks(),
//                "Kafka-Source-" + topics.get(i)
//            );
//
//            if (i == 0) {
//                unionStream = stream;
//            } else {
//                unionStream = unionStream.union(stream);
//            }
//        }
//        return unionStream;
//    }
//
//
//
//    // 内存存储Sink实现
//    private static class InMemoryListSink extends RichSinkFunction<poiS> {
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
//    }
//
//}