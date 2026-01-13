package com.ljj.toshow.demos.bean;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ljj.toshow.demos.pojo.poiS;
import jakarta.annotation.PreDestroy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import com.github.luben.zstd.ZstdInputStream;
@Component
public class StartupRunner implements CommandLineRunner {

    // 使用 @Value 注解从配置中获取 Kafka 参数
    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.topics}")
    private String topicsConfig;

    private List<String> topics;

    private static final int VEHICLE_LIFETIME_MS = 400;  // 车辆存活时间5秒


    // 优化数据结构：复合存储车辆信息
    public static final ConcurrentHashMap<String, VehicleData> VEHICLE_STORE = new ConcurrentHashMap<>();

    // 记录最近出现的车辆ID集合
    public static final Set<String> RECENTLY_SEEN_VEHICLES = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // 清理线程控制
    private volatile boolean cleanupRunning = true;
    private Thread cleanupThread;

    @Override
    public void run(String... args) throws Exception {
        // 解析 topics 配置（逗号分隔的字符串）
        if (topicsConfig != null && !topicsConfig.isEmpty()) {
            this.topics = Arrays.asList(topicsConfig.split(","));
        } else {
            throw new IllegalArgumentException("Kafka topics must be configured via kafka.topics property");
        }

        // 验证配置
        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("Kafka brokers must be configured via kafka.brokers property");
        }

        System.out.println("Starting Flink job with configuration:");
        System.out.println("  Kafka Brokers: " + brokers);
        System.out.println("  Kafka Topics: " + topics);

        // 在单独的线程中启动 Flink 作业，避免阻塞 Spring Boot 启动
        Thread flinkThread = new Thread(() -> {
            try {
                System.out.println("Flink 作业线程启动...");
                startFlinkJob();
            } catch (Exception e) {
                System.err.println("Flink 作业启动失败: " + e.getMessage());
                e.printStackTrace();
            }
        });
        flinkThread.setName("Flink-Job-Thread");
        flinkThread.setDaemon(false); // 设置为非守护线程，确保应用关闭时线程也会关闭
        flinkThread.start();
        
        System.out.println("Flink 作业已在后台线程中启动");
    }

    private void startFlinkJob() throws Exception {
        System.out.println("正在初始化 Flink 执行环境...");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 适当提高并行度
        // 配置执行环境以减少延迟
        env.getConfig().setLatencyTrackingInterval(100); // 设置延迟跟踪间隔
        env.setBufferTimeout(0); // 禁用缓冲，确保数据立即通过

        String groupId = "flink-group";
        System.out.println("正在构建 Kafka 数据流...");
        DataStream<String> unionStream = buildKafkaStream(env, brokers, groupId, topics);

        // 添加压缩类型标记
        System.out.println("正在配置数据解压阶段...");
        DataStream<String> decompressedStream = unionStream
            .map(new CompressionMapper())
            .name("decompression-stage")
            .returns(String.class);

        System.out.println("正在配置数据解析阶段...");
        DataStream<poiS> pointStream = decompressedStream
            .flatMap(new PoiFlatMap())
            .name("poi-parsing-stage");

        // 添加缓存管理
        System.out.println("正在配置数据存储阶段...");
        pointStream.addSink(new VehicleSink())
            .name("vehicle-sink");

        // 启动清理线程
        System.out.println("正在启动清理线程...");
        startCleanupThread();

        System.out.println("正在执行 Flink 作业...");
        env.execute("Optimized Vehicle Points Processing");
    }

    // Kafka源构建
    private DataStream<String> buildKafkaStream(StreamExecutionEnvironment env,
                                                String brokers, String groupId,
                                                List<String> topics) {
        System.out.println("构建 Kafka 数据源:");
        System.out.println("  Brokers: " + brokers);
        System.out.println("  Topics: " + topics);
        System.out.println("  GroupId: " + groupId);
        
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(brokers)
            .setTopics(topics)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest()) // 只消费启动后的新消息，忽略历史消息
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        System.out.println("Kafka 数据流已创建，等待数据...");
        return stream;
    }

    // 压缩处理统一封装
    private static class CompressionMapper extends RichMapFunction<String, String> {

        @Override
        public String map(String record) throws Exception {
            try {
                if (record == null || record.isEmpty()) {
                    System.err.println("CompressionMapper: 收到空记录");
                    return "{}";
                }
                
                System.out.println("CompressionMapper: 收到数据，长度=" + record.length() + ", 前50字符: " + 
                    (record.length() > 50 ? record.substring(0, 50) : record));
                
                if (record.startsWith("ZSTD:")) {
                    System.out.println("检测到 ZSTD 压缩数据，开始解压...");
                    return decompressZstd(record.substring(5));
                } else if (record.startsWith("GZIP:")) {
                    System.out.println("检测到 GZIP 压缩数据，开始解压...");
                    return decompressGzip(record.substring(5));
                } else {
                    System.out.println("未压缩数据，直接返回");
                }
                return record;
            } catch (Exception e) {
                logError("解压失败", record, e);
                return "{}";
            }
        }

      private String decompressZstd(String base64Data) throws IOException {
        byte[] compressed = Base64.getDecoder().decode(base64Data);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
             ZstdInputStream zis = new ZstdInputStream(bis);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int len;
            while ((len = zis.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
            // 修正：使用字节数组和字符集创建字符串
            return new String(baos.toByteArray(), StandardCharsets.UTF_8);
        }
    }

        private String decompressGzip(String base64Data) throws IOException {
            byte[] compressedBytes = Base64.getDecoder().decode(base64Data);
            try (ByteArrayInputStream bis = new ByteArrayInputStream(compressedBytes);
                 GZIPInputStream gzis = new GZIPInputStream(bis);
                 ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = gzis.read(buffer)) != -1) {
                    baos.write(buffer, 0, bytesRead);
                }
                return new String(baos.toByteArray(), StandardCharsets.UTF_8);
            }
        }

        private void logError(String msg, String record, Exception e) {
            System.err.println(msg + ": " + e.getMessage());
            System.err.println("原始数据: " + record.substring(0, Math.min(record.length(), 100)) + "...");
        }
    }

    // 数据处理优化
    private static class PoiFlatMap implements FlatMapFunction<String, poiS> {
        private static long batchCounter = 0; // 批次计数器
        
        // Java 8 兼容的字符串重复方法
        private String repeat(String str, int count) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < count; i++) {
                sb.append(str);
            }
            return sb.toString();
        }

        @Override
        public void flatMap(String jsonString, Collector<poiS> out) {
            try {
                // 添加调试日志：打印接收到的原始数据
                if (jsonString == null || jsonString.isEmpty()) {
                    System.err.println("警告: 收到空的jsonString");
                    return;
                }
                
                // 检查是否是空JSON对象
                if (jsonString.trim().equals("{}") || jsonString.trim().isEmpty()) {
                    System.err.println("收到空JSON对象，跳过");
                    return;
                }
                
                batchCounter++;
                
                JSONObject jsonObject = JSON.parseObject(jsonString);
                
                // 统一按 pathList 格式处理
                JSONArray pathList = jsonObject.getJSONArray("pathList");
                String timeStampStr = jsonObject.getString("timeStamp");
                
                if (pathList != null && pathList.size() > 0) {
                    // pathList 格式：包含 pathList 数组
                    int successCount = 0;
                    int errorCount = 0;
                    
                    // 简化输出：只显示批次关键信息
                    System.out.println("【批次 #" + batchCounter + "】时间戳: " + (timeStampStr != null ? timeStampStr : "未提供") + " | 车辆数: " + pathList.size());
                    
                    // 收集所有字段名（只收集一次，避免重复输出）
                    Set<String> allFields = new HashSet<>();
                    
                    for (int i = 0; i < pathList.size(); i++) {
                        try {
                            JSONObject point = pathList.getJSONObject(i);
                            if (point != null) {
                                // 收集所有字段名
                                allFields.addAll(point.keySet());
                                
                                preprocessPoint(point);
                                poiS poi = convertToPoiS(point, timeStampStr);

                                String vehicleKey = poi.getId() + "_" + poi.getName();
                                RECENTLY_SEEN_VEHICLES.add(vehicleKey);

                                // 从原始 point 对象中提取里程和匝道信息
                                String plateNo = poi.getName();
                                Long vehicleId = poi.getId();
                                Double mileage = point.getDouble("mileage");
                                String ramp = point.getString("ramp");
                                String rampStake = point.getString("rampStake");
                                String stakeId = poi.getStakeId();
                                
                                // 简化输出：只显示关键信息
                                String mileageStr = mileage != null ? String.format("%.1f", mileage) + "m" : "未知";
                                String rampStr = ramp != null ? ramp : (stakeId != null && !stakeId.isEmpty() ? stakeId.split("-")[0].replace("K", "") : "未知");
                                
                                // 显示 specialFlag 字段的状态：区分不存在、null、有值
                                String specialFlagStr;
                                if (!point.containsKey("specialFlag")) {
                                    specialFlagStr = "字段不存在";
                                } else {
                                    Object specialFlagValue = point.get("specialFlag");
                                    if (specialFlagValue == null) {
                                        specialFlagStr = "null";
                                    } else if (specialFlagValue instanceof Number) {
                                        int specialFlagInt = ((Number) specialFlagValue).intValue();
                                        specialFlagStr = specialFlagInt == 99 ? "99(预测点)" : String.valueOf(specialFlagInt);
                                    } else if (specialFlagValue instanceof String) {
                                        // 处理字符串类型
                                        String specialFlagString = (String) specialFlagValue;
                                        try {
                                            int specialFlagInt = Integer.parseInt(specialFlagString);
                                            specialFlagStr = specialFlagInt == 99 ? "99(预测点)" : specialFlagString;
                                        } catch (NumberFormatException e) {
                                            specialFlagStr = "字符串(" + specialFlagString + ")";
                                        }
                                    } else {
                                        specialFlagStr = "类型错误(" + specialFlagValue.getClass().getSimpleName() + ")";
                                    }
                                }
                                
                                System.out.println("  " + plateNo + " | ID:" + vehicleId + " | 匝道:" + rampStr + " | 里程:" + mileageStr + " | specialFlag:" + specialFlagStr + " | 桩号:" + (rampStake != null ? rampStake : stakeId));
                                
                                out.collect(poi);
                                successCount++;
                            }
                        } catch (Exception e) {
                            errorCount++;
                            System.err.println("  ❌ 处理车辆点 [" + (i + 1) + "] 错误: " + e.getMessage());
                            if (errorCount <= 3) {
                                System.err.println("     错误点数据: " + pathList.getJSONObject(i));
                            }
                        }
                    }
                    
                    // 输出所有字段名（按字母顺序排序）
                    if (!allFields.isEmpty()) {
                        List<String> sortedFields = new ArrayList<>(allFields);
                        Collections.sort(sortedFields);
                        System.out.println("  字段列表: " + String.join(", ", sortedFields));
                    }
                    
                    if (errorCount > 0) {
                        System.out.println("  处理结果: 成功 " + successCount + " 个，错误 " + errorCount + " 个");
                    }
                    System.out.println("");
                } else {
                    // 不是 pathList 格式，输出错误信息
                    System.err.println("⚠ 警告: 数据格式不符合要求，必须是 pathList 格式");
                    System.err.println("   期望格式: {\"timeStamp\": \"...\", \"pathList\": [...]}");
                    System.err.println("   实际JSON结构: " + jsonObject.keySet());
                    System.err.println("   实际JSON内容: " + jsonObject);
                    System.out.println(repeat("=", 80) + "\n");
                }
            } catch (Exception e) {
                System.err.println("❌ JSON解析错误: " + e.getMessage());
                if (jsonString != null) {
                    System.err.println("   原始内容: " + jsonString.substring(0, Math.min(jsonString.length(), 500)) + "...");
                } else {
                    System.err.println("   原始内容: null");
                }
                e.printStackTrace();
                System.out.println(repeat("=", 80) + "\n");
            }
        }

        private void preprocessPoint(JSONObject point) {
            // 添加默认值处理
            if (!point.containsKey("direction")) {
                point.put("direction", 1);
            }
            if (!point.containsKey("plateNo")) {
                point.put("plateNo", "默A00000");
            }
            // stakeId 可以为空，使用默认值
            if (!point.containsKey("stakeId")) {
                point.put("stakeId", "");
            }
            // 验证必要字段
            if (!point.containsKey("plateNo")) {
                throw new IllegalArgumentException("缺少必要字段 plateNo: " + point);
            }
            // id 字段验证
            if (!point.containsKey("id")) {
                // 如果没有id，使用时间戳生成一个
                point.put("id", System.currentTimeMillis());
            }
        }

        private poiS convertToPoiS(JSONObject point,String timeStampStr) {
            // 获取source字段，如果不存在则默认为null
            Integer source = null;
            if (point.containsKey("source")) {
                Object sourceValue = point.get("source");
                if (sourceValue instanceof Number) {
                    source = ((Number) sourceValue).intValue();
                }
            }
            
            // 获取specialFlag字段，如果不存在则默认为null
            // 支持数字类型和字符串类型（如 "99"）
            Integer specialFlag = null;
            if (point.containsKey("specialFlag")) {
                Object specialFlagValue = point.get("specialFlag");
                if (specialFlagValue instanceof Number) {
                    specialFlag = ((Number) specialFlagValue).intValue();
                } else if (specialFlagValue instanceof String) {
                    // 处理字符串类型，尝试转换为整数
                    try {
                        specialFlag = Integer.parseInt((String) specialFlagValue);
                    } catch (NumberFormatException e) {
                        // 如果无法转换为整数，保持为 null
                        specialFlag = null;
                    }
                }
            }
            
            return new poiS(
                point.getDouble("longitude"),
                point.getDouble("latitude"),
                point.getString("plateNo"),
                point.getString("stakeId"),
                point.getLong("id"),
                point.getInteger("direction"),
                timeStampStr,
                source,
                specialFlag
            );
        }
    }

    // 存储结构优化
    public static class VehicleData {
        public poiS vehicle;
        public long lastUpdate;
        public boolean active;

        public VehicleData(poiS vehicle) {
            this.vehicle = vehicle;
            this.lastUpdate = System.currentTimeMillis();
            this.active = true;
        }

        public void update(poiS newVehicle) {
            // 更新车辆对象的所有字段，确保数据同步
            if (this.vehicle != null) {
                this.vehicle.setLongitude(newVehicle.getLongitude());
                this.vehicle.setLatitude(newVehicle.getLatitude());
                this.vehicle.setName(newVehicle.getName());
                this.vehicle.setStakeId(newVehicle.getStakeId());
                this.vehicle.setId(newVehicle.getId());
                this.vehicle.setDirection(newVehicle.getDirection());
                this.vehicle.setTimeStampStr(newVehicle.getTimeStampStr());
                this.vehicle.setSource(newVehicle.getSource()); // 更新source字段
                this.vehicle.setSpecialFlag(newVehicle.getSpecialFlag()); // 更新specialFlag字段
            } else {
                this.vehicle = newVehicle;
            }
            this.lastUpdate = System.currentTimeMillis();
            this.active = true;
        }

        public void deactivate() {
            this.active = false;
        }
    }

    // 缓存管理增强
    public static class VehicleSink extends RichSinkFunction<poiS> {

        @Override
        public void open(Configuration parameters) {
            // 可以添加指标监控
        }

        @Override
        public void invoke(poiS value, Context context) {
            if (value == null || value.getName() == null) {
                System.err.println("VehicleSink: 收到无效数据，value为null或name为null");
                return;
            }

            String key = "show_" + value.getName() + "_" + value.getId();

            // 统一处理：无论 direction 是什么值，都正常存储和显示
            // 存在则更新，不存在则创建（不打印日志，减少输出）
            VEHICLE_STORE.compute(key, (k, v) -> {
                if (v == null) {
                    return new VehicleData(value);
                } else {
                    // 更新车辆数据
                    v.update(value);
                    return v;
                }
            });
        }
    }

    // Java 8 兼容的字符串重复方法
    private String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
    
    // 启动清理线程（重点修改部分）
    private void startCleanupThread() {
        cleanupThread = new Thread(() -> {
            while (cleanupRunning) {
                try {
                    long startTime = System.currentTimeMillis();
                    int removedCount = 0;
                    int preservedCount = 0;

                    // 避免频繁全表扫描
                    TimeUnit.MILLISECONDS.sleep(1000);

                    long currentTime = System.currentTimeMillis();
                    Iterator<Map.Entry<String, VehicleData>> it = VEHICLE_STORE.entrySet().iterator();

                    // 准备处理不在最近可见集合中的车辆
                    Set<String> vehiclesToRemove = new HashSet<>();

                    while (it.hasNext()) {
                        Map.Entry<String, VehicleData> entry = it.next();
                        String key = entry.getKey();
                        VehicleData data = entry.getValue();

                        // 提取车辆唯一标识 (ID_PlateNo)
                        String[] parts = key.split("_");
                        String vehicleId = parts.length > 2 ? parts[2] : "";
                        String plateNo = parts.length > 1 ? parts[1] : "";
                        String vehicleKey = vehicleId + "_" + plateNo;

                        // 清理策略：只清除不在最近可见列表中的车辆
                        if (!RECENTLY_SEEN_VEHICLES.contains(vehicleKey) &&
                            currentTime - data.lastUpdate > VEHICLE_LIFETIME_MS) {

                            vehiclesToRemove.add(key);
                            removedCount++;
                        } else {
                            preservedCount++;
                        }
                    }

                    // 批量移除不再出现的车辆
                    for (String key : vehiclesToRemove) {
                        VEHICLE_STORE.remove(key);
                    }

                    // 清空"最近可见"集合并为下一轮收集做准备
                    RECENTLY_SEEN_VEHICLES.clear();

                    long duration = System.currentTimeMillis() - startTime;
                    // 调试信息
                    String separator = repeatString("─", 80);
                    System.out.println("\n" + separator);
                    System.out.printf("【清理任务】移除: %d 辆 | 保留: %d 辆 | 耗时: %dms%n",
                            removedCount, preservedCount, duration);
                    System.out.println(separator + "\n");

                } catch (InterruptedException e) {
                    if (cleanupRunning) {
                        System.err.println("清理线程异常中断: " + e.getMessage());
                    }
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println("清理过程中出错: " + e.getMessage());
                }
            }
        });

        cleanupThread.setName("Vehicle-Cleanup-Thread");
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    @PreDestroy
    public void cleanup() {
        cleanupRunning = false;
        if (cleanupThread != null) {
            cleanupThread.interrupt();
            try {
                cleanupThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}