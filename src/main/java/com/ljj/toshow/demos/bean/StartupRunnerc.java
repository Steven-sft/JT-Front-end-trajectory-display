import com.ljj.toshow.demos.pojo.TrafficEventUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

//@Bean
//    @Qualifier("stakeAssign1")
//    public TrafficEventUtils.StakeAssignment stakeAssign1(@Value("${converter1.file}") String filePath) throws IOException, IOException {
//        return new TrafficEventUtils.StakeAssignment(filePath);
//    }
//
//    @Bean
//    @Qualifier("stakeAssign2")
//    public TrafficEventUtils.StakeAssignment stakeAssign2(@Value("${converter2.file}") String filePath) throws IOException {
//        return new TrafficEventUtils.StakeAssignment(filePath);
//    }
//package com.ljj.toshow.demos.bean;
//
//import com.alibaba.fastjson2.JSON;
//import com.alibaba.fastjson2.JSONObject;
//import com.ljj.toshow.demos.pojo.TrafficEventUtils;
//import com.ljj.toshow.demos.pojo.Utils.PathPoint;
//import com.ljj.toshow.demos.pojo.Utils.PathPointData;
//import com.ljj.toshow.demos.pojo.Utils.PathTData;
//import com.ljj.toshow.demos.pojo.poiS;
//import com.ljj.toshow.demos.tools.myTools;
//import javafx.util.Pair;
//import lombok.Getter;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.util.Collector;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.CommandLineRunner;
//import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
//import org.springframework.data.redis.core.RedisTemplate;
//import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
//import org.springframework.data.redis.serializer.StringRedisSerializer;
//import org.springframework.stereotype.Component;
//
//import java.io.Serializable;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.format.DateTimeFormatter;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.TimeUnit;
//
//
//@Component
//public class StartupRunnerc implements CommandLineRunner {
//
//    public static RedisTemplate<String, poiS> redisTemplate;
//    @Getter
//    private static TrafficEventUtils.MileageConverter mileageConverter1 = null;
//    @Getter
//    private static TrafficEventUtils.MileageConverter mileageConverter2 = null;
//    @Getter
//    private final TrafficEventUtils.StakeAssignment stakeAssign1;
//    @Getter
//    private final TrafficEventUtils.StakeAssignment stakeAssign2;
//    private  String brokers;
//    private List<String> topics; // 改为非 final，允许动态注入
//    private static final int WINDOW_SIZE = 20;//用来预测的窗口大小
//    private static final Map<Long, PathPointData> pointMap = new ConcurrentHashMap<>();
//    private static final Map<Long, PathPointData> JizhanPointMap = new ConcurrentHashMap<>();//雷视数据获取到的匝道上的所有车
//    static boolean firstEnter = true;
//    static Map<Long, PathPoint> lastMap = new ConcurrentHashMap<>();
//    static Map<Long, PathPoint> tempMap = new ConcurrentHashMap<>();
//
//    //    carid  是否在路上  数据缺失了几次
//    static Map<Pair<Long, String>, String> zaMap = new ConcurrentHashMap<>();
//    //       carid  carNumber  匝道编号
//    private static final long mainRoadMinMillage = 0;//主路上的最小里程
//    private static final long mainRoadMaxMillage = 1111111111;//主路上的最大里程
//    private static String pathTimeStamp = "";
//    private static float predictedSpeed = 0;//预测速度
//    private static double distanceDiff = 0;
//    private static long pathTime = 0;
//    private static int tcount = 0;
//    private static long t1 = 0;
//    private static boolean tb1 = true;
//    private static boolean tb2 = true;
//    private static long t2 = 0;
//    private static long t3 = 0;
//    private static long temp = 0;
//    private static long dis = 0;
//    private static int newscount = 0;
//    @Autowired
//    public StartupRunnerc(
//        RedisTemplate<String, poiS> redisTemplate,
//        @Qualifier("mileageConverter1") TrafficEventUtils.MileageConverter converter1,
//        @Qualifier("mileageConverter2") TrafficEventUtils.MileageConverter converter2,
//        @Qualifier("stakeAssign1") TrafficEventUtils.StakeAssignment sa1,
//        @Qualifier("stakeAssign2") TrafficEventUtils.StakeAssignment sa2,
//        @Value("${kafka.brokers}") String brokers
//    ) {
//        this.redisTemplate = redisTemplate;
//        this.mileageConverter1 = converter1;
//        this.mileageConverter2 = converter2;
//        this.stakeAssign1 = sa1;
//        this.stakeAssign2 = sa2;
//    }
//
//    @Override
//    public void run(String... args) throws Exception {
//        if (args.length < 2) {
//            throw new IllegalArgumentException("Usage: java -jar your-app.jar <broker> <topic1> [topic2...]");
//        }
//        this.brokers =  args[0];
//        this.topics = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));
//        startFlinkJob();
//    }
//
//
//    private void startFlinkJob() throws Exception {
//         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
////         Kafka配置
////        String brokers = "100.65.38.40:9092";
//
////List<String> topics = Arrays.asList("fiberDataTest1", "fiberDataTest2", "fiberDataTest3");
//        // 构建Kafka数据流
//          // 配置Kafka连接信息
//            String brokers = "100.65.38.139:9092";
//            String groupId = "flink_consumer_group";
//            List<String> topics = Arrays.asList("fiberDataTest1", "fiberDataTest2", "fiberDataTest3");
////            List<String> topics = Collections.singletonList("news-topic");
////            List<String> topics = Collections.singletonList("MergedPathData.sceneTest.1");
//            // 创建Kafka数据源
//            KafkaSource<String> source = KafkaSource.<String>builder()
//                    .setBootstrapServers(brokers)
//                    .setTopics(topics)
//                    .setGroupId(groupId)
//                    .setStartingOffsets(OffsetsInitializer.latest())
//                    .setValueOnlyDeserializer(new SimpleStringSchema())
//                    .setProperty("message.max.bytes", "16777216")
//                    .setProperty("max.partition.fetch.bytes", "16777216")
//                    .build();
//
//            // 从Kafka读取数据
//            DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source1");
//            DataStream<PathTData> parsedStream = kafkaStream
//                    .flatMap((String jsonStr, Collector<PathTData> out) -> {
//                        try {
//                            PathTData data = null;
//                            //验证，如果json的前几位是timestamp，则认为是mergedata
//                            data = JSON.parseObject(jsonStr, PathTData.class);
//                            out.collect(data);
//                            System.out.println("data:" + data);
////                            }
//                        } catch (Exception e) {
//                            System.err.println("JSON解析失败: " + jsonStr);
//                        }
//                    }).returns(PathTData.class).keyBy(PathTData::getTime);
//            SingleOutputStreamOperator<PathTData> endPathTDataStream = parsedStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
//                @Override//5.56   33.76  86.64
//                public void flatMap(PathTData pathTData, Collector<PathTData> collector) throws Exception {
//                    List<PathPoint> list=new ArrayList<>();
//                    pathTimeStamp=pathTData.getTimeStamp();
//                    pathTime= pathTData.getTime();
//                    PathTData pathTData1 = new PathTData();
//                    pathTData.setTime(pathTime);
//                    pathTData.setTimeStamp(pathTimeStamp);
//                    pathTData.setPathNum(pathTData1.getPathNum());
//                    pathTData.setWaySectionId(pathTData1.getWaySectionId());
//                    pathTData.setWaySectionName(pathTData1.getWaySectionName());
//
//                    try {
//                        // 尝试按三位毫秒格式解析
//                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
//                        LocalDateTime localDateTime = LocalDateTime.parse(pathTData.getTimeStamp(), formatter);
//                        temp = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//                    } catch (Exception e) {
//                        // 若三位毫秒格式解析失败，尝试按两位毫秒格式解析
//                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
//                        LocalDateTime localDateTime = LocalDateTime.parse(pathTData.getTimeStamp(), formatter);
//                        temp = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//                    }
//                    if (!pathTData.getPathList().isEmpty()) {
//                        if (firstEnter) {
//                            firstEnterInitializePointMapAndlastMap(pathTData);
//                            firstEnter = false;
//                        } else {
//                            putNowDataIntoTempMap(pathTData);
//                        }
//
//                        //遍历lastMap，看是否有车没了,也就是lastMap有，tempMap目前没有
//                        for (Map.Entry<Long, PathPoint> entry : lastMap.entrySet()) {
//                            long key = entry.getKey();
//                            String stake ="";
//                            PathPoint pp=tempMap.get(key);
//                            PathPoint value=new PathPoint();
//                            //问题：车不能出边界
//                            if (pp == null) {//前面有车但是当前没车
//                                value = predictMainRoadNextOne_UpdataPointMap(key);
//                            }else{
//                                if(pp.getStakeId()==null||pp.getStakeId().isEmpty()){//处理缺失
//                                    TrafficEventUtils.StakeAssignment stakeAssign = (pp.getDirection() == 1)
//                                            ? stakeAssign1 : stakeAssign2;
//                                    stake = stakeAssign.findInsertionIndex(pp.getLongitude(), pp.getLatitude());
//                                    pointMap.get(key).setStakeId(stake);
//                                    tempMap.get(key).setStakeId(stake);
//                                    //问题：经纬度null判断
//                                }else if(pp.getLatitude()==0||pp.getLongitude()==0){
//                                    TrafficEventUtils.MileageConverter converter = (pp.getDirection() == 1)
//                                            ? mileageConverter1 : mileageConverter2;
//                                    double[] d=converter.findCoordinate(stakeToMileage(stake)).getLnglat();
//                                    pointMap.get(key).setLatitude(d[1]);
//                                    pointMap.get(key).setLongitude(d[0]);
//                                    tempMap.get(key).setLatitude(d[1]);
//                                    tempMap.get(key).setLongitude(d[0]);
//                                }
//                                value=tempMap.get(key);
//                            }
//                            list.add(value);
//
//                        }
//                        pathTData1.setPathList(list);
////                        System.out.println(list);
//                        collector.collect(pathTData1);
//                        //mark:防撞
//                        lastMap=tempMap;
//                    }//pathlist.empty
//                }//flatMap
//            });
//            env.execute("Flink completion");
//
//         // 处理数据流时使用静态内部类并传递转换器
//    DataStream<poiS> pointStream = unionStream
//        .flatMap(new PoiFlatMap(mileageConverter1, mileageConverter2));
//
//        // 添加内存存储Sink
//        pointStream.addSink(new RedisSink());
//
//        env.execute("Real-time Vehicle Points Processing");
//    }
//
//     // Kafka流构建方法
//    public static DataStream<String> buildKafkaStream(StreamExecutionEnvironment env,
//                                                      String brokers, String groupId,
//                                                      List<String> topics) {
//      KafkaSource<String> source = KafkaSource.<String>builder()
//            .setBootstrapServers(brokers)
//            .setTopics(topics) // 直接使用列表
//            .setGroupId(groupId)
//            .setStartingOffsets(OffsetsInitializer.latest())
//            .setValueOnlyDeserializer(new SimpleStringSchema())
//            .build();
//
//    return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//    }
//private static PathPoint predictMainRoadNextOne_UpdataPointMap(long key) {
//        PathPointData data=pointMap.get(key);
//        LinkedList<Float> spw=data.getSpeedWindow();
//        predictedSpeed=calculateMovingAverage(spw);
//        spw.addLast(predictedSpeed);
//        if(spw.size()>WINDOW_SIZE)spw.removeFirst();
//        double newTpointno=0;
//        distanceDiff = myTools.calculateDistance(predictedSpeed, 200);
//        if(data.getDirection()==1) {
//            newTpointno = data.getMileage() + distanceDiff; // 更新里程点
//        }else {
//            newTpointno = data.getMileage() - distanceDiff; // 更新里程点
//        }
//        if(newTpointno<mainRoadMinMillage||newTpointno>mainRoadMaxMillage)return null;
//        String stake=data.getStakeId();
//        String newStake=MileageToStake((int)(stakeToMileage(stake)+distanceDiff));
//        TrafficEventUtils.MileageConverter converter = (data.getDirection() == 1) ? mileageConverter1 : mileageConverter2;
//        double[] d=converter.findCoordinate(stakeToMileage(newStake)).getLnglat();
//        //问题；角度
//        double carangle=89;
//        data.setCarAngle(carangle);
//        data.setMileage((int)newTpointno);
//        data.setSpeed(predictedSpeed);
////        data.setTimeStamp(pathTimeStamp);//未接收到，不更新
//        data.setLatitude(d[1]);
//        data.setLongitude(d[0]);
//        data.setSpeedWindow(spw);
//        data.setStakeId(newStake);
//        pointMap.put(key, data);
//        PathPoint pp=PDToPP(data);
//        myTools.printmergePoint(pp);
//        return pp;
//    }
//    private static int stakeToMileage(String stakeId) {
//        return Integer.parseInt(stakeId.split("\\+")[0].substring(1)) * 1000 + Integer.parseInt(stakeId.split("\\+")[1]);
//    }
//    private static String MileageToStake(int newMileage) {
//        return newMileage/1000+"+"+(newMileage-(newMileage/1000*1000));
//    }
//    private static float calculateMovingAverage(LinkedList<Float> speedWindow) {
//        return (float) speedWindow.stream()
//                .mapToDouble(Float::doubleValue)
//                .average()
//                .orElse(Double.NaN);
//    }
//    private static void putNowDataIntoTempMap(PathTData pathTData){
//        List<PathPoint> p=pathTData.getPathList();
//        for(PathPoint m:p) tempMap.put(m.getId(),m);
//
//    }
//
//    private static void firstEnterInitializePointMapAndlastMap (PathTData pathTData){
//        List<PathPoint> p=pathTData.getPathList();
//        for(PathPoint m:p){
//            lastMap.put(m.getId(),m);
//            PathPointData pp=PPToPD(m);
//            pp.setLastReceivedTime(pathTData.getTime());
//            pp.getSpeedWindow().add(m.getSpeed());
//            pointMap.put(m.getId(),pp);
//        }
//    }
//    private static void firstEnterInsertPointMap(PathPoint p){
//            PathPointData pp=PPToPD(p);
//            //temp是当前时间
//            pp.setLastReceivedTime(temp);
//            pp.getSpeedWindow().add(p.getSpeed());
//            pointMap.put(p.getId(),pp);
//
//    }
//    private static PathPoint PDToPP(PathPointData Point) {
//        PathPoint pathPoint = new PathPoint();
//
//        pathPoint.setMileage(Point.getMileage());
//        pathPoint.setId(Point.getId());
//        pathPoint.setSpeed(Point.getSpeed());
//        pathPoint.setDirection(Point.getDirection());
//        pathPoint.setLatitude(Point.getLatitude());
//        pathPoint.setLongitude(Point.getLongitude());
//        pathPoint.setLaneNo(Point.getLaneNo());
//        pathPoint.setCarAngle(Point.getCarAngle());
//        pathPoint.setOriginalColor(Point.getOriginalColor());
//        pathPoint.setPlateColor(Point.getPlateColor());
//        pathPoint.setStakeId(Point.getStakeId());
//        pathPoint.setPlateNo(Point.getPlateNo());
//        pathPoint.setOriginalType(Point.getOriginalType());
//        pathPoint.setVehicleType(Point.getVehicleType());
//        pathPoint.setTimeStamp(Point.getTimeStamp());
//        return pathPoint;
//    }
//    private static PathPointData PPToPD(PathPoint Point) {
//        PathPointData pathPoint = new PathPointData();
//        pathPoint.setMileage(Point.getMileage());
//        pathPoint.setId(Point.getId());
//        pathPoint.setSpeed(Point.getSpeed());
//        pathPoint.setDirection(Point.getDirection());
//        pathPoint.setLatitude(Point.getLatitude());
//        pathPoint.setLongitude(Point.getLongitude());
//        pathPoint.setLaneNo(Point.getLaneNo());
//        pathPoint.setCarAngle(Point.getCarAngle());
//        pathPoint.setOriginalColor(Point.getOriginalColor());
//        pathPoint.setPlateColor(Point.getPlateColor());
//        pathPoint.setStakeId(Point.getStakeId());
//        pathPoint.setPlateNo(Point.getPlateNo());
//        pathPoint.setOriginalType(Point.getOriginalType());
//        pathPoint.setVehicleType(Point.getVehicleType());
//        pathPoint.setTimeStamp(Point.getTimeStamp());
//        pathPoint.setSpeedWindow(new LinkedList<>());
//
//        return pathPoint;
//    }
//// 新增静态内部类实现FlatMapFunction
//private static class PoiFlatMap implements FlatMapFunction<String, poiS>, Serializable {
//    private final TrafficEventUtils.MileageConverter mileageConverter1;
//    private final TrafficEventUtils.MileageConverter mileageConverter2;
//
//    public PoiFlatMap(TrafficEventUtils.MileageConverter converter1,
//                      TrafficEventUtils.MileageConverter converter2) {
//        this.mileageConverter1 = converter1;
//        this.mileageConverter2 = converter2;
//    }
//
//    @Override
//    public void flatMap(String jsonString, Collector<poiS> out) {
//        try {
//            JSONObject jsonObject = JSON.parseObject(jsonString);
//            for (JSONObject JSONpoint : JSON.parseArray(jsonObject.getString("pathList"), JSONObject.class)) {
//                preprocessPoint(JSONpoint);
////                System.out.println("json: "+JSONpoint.toJSONString());
//                poiS poi = convertToPoiS(JSONpoint);
////                System.out.println("poi: "+poi);
//                out.collect(poi);
//            }
//        } catch (Exception e) {
//            System.err.println("处理数据时出错: " + e.getMessage());
//            }
//    }
//
//    private void preprocessPoint(JSONObject point) {
//        if (!point.containsKey("specialFlag")) {
//            point.put("specialFlag", "0");
//        }
//    }
//
//    private poiS convertToPoiS(JSONObject point) {
//        poiS p = new poiS();
//        p.setStakeId(point.getString("stakeId"));
//        p.setName(point.getString("plateNo"));
//        p.setDirection(point.getInteger("direction"));
//
//        if(point.getDouble("latitude")==null||point.getDouble("longitude")==null){
//             TrafficEventUtils.MileageConverter converter = (p.getDirection() == 1)
//                ? mileageConverter1 : mileageConverter2;
//             double[] lnglat = converter.findCoordinate(point.getInteger("mileage")).getLnglat();
//             p.setLatitude(lnglat[1]);
//             p.setLongitude(lnglat[0]);
//        }else{
//            p.setLatitude(point.getDouble("latitude"));
//            p.setLongitude(point.getDouble("longitude"));
//        }
//
//
//
////                p.setLatitude(point.getDouble("latitude"));
////        p.setLongitude(point.getDouble("longitude"));
//        return p;
//    }
//}
//    public static class RedisSink extends RichSinkFunction<poiS> {
//
//
//    @Override
//    public void open(Configuration parameters) {
//        LettuceConnectionFactory factory = new LettuceConnectionFactory("100.65.38.139", 6379);
//        factory.afterPropertiesSet();
//
//        redisTemplate = new RedisTemplate<>();
//        redisTemplate.setConnectionFactory(factory);
//        redisTemplate.setKeySerializer(new StringRedisSerializer());
//        // 使用 JSON 序列化器
//        redisTemplate.setValueSerializer(new GenericJackson2JsonRedisSerializer());
//        redisTemplate.afterPropertiesSet();
//    }
//
//    @Override
//    public void invoke(poiS value, Context context) {
//        // 直接存储对象，序列化器会自动转换为 JSON
//        redisTemplate.opsForValue().set(
//            "v" + value.getName(),
//            value,
//            30, TimeUnit.SECONDS
//        );
////        System.out.println("成功写入 Redis: " + value.getName());
//    }
//}
////    // 内存存储Sink实现
////    public static class InMemoryListSink extends RichSinkFunction<poiS> {
////
////        @Override
////        public void open(Configuration parameters) {
////
////            // 初始化操作（如有需要）
////        }
////
////        @Override
////        public void invoke(poiS value, Context context) {
//////            System.out.println("start invoke,value:" + value);
////            resMap.put(value.getName(), value);
////            // 调试输出
////            if (resMap.size() % 1000 == 0) {
////                System.out.println("当前存储点数: " + resMap.size());
////            }
////        }
////
////
////    }
//
//}
