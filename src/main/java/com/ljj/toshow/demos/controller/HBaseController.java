package com.ljj.toshow.demos.controller;

import com.ljj.toshow.demos.bean.StartupRunner;
import com.ljj.toshow.demos.pojo.poiS;
import com.ljj.toshow.demos.service.HBaseService;
import com.ljj.toshow.demos.tools.BaseStationReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.util.*;

@Slf4j
@RestController
@CrossOrigin(origins = "*")
public class HBaseController {

    @Autowired
    private HBaseService hbaseService;

  @GetMapping("/points")
    public List<poiS> getVehicles() {
        // 从优化后的VehicleStore获取活跃车辆
        List<poiS> activeVehicles = new ArrayList<>(StartupRunner.VEHICLE_STORE.size());
        int totalCount = 0;
        int activeCount = 0;
        
        for (StartupRunner.VehicleData data : StartupRunner.VEHICLE_STORE.values()) {
            totalCount++;
            if (data.active && data.vehicle != null) {
                // 创建新的poiS对象，确保返回最新数据，避免引用问题
                poiS vehicle = new poiS();
                vehicle.setLongitude(data.vehicle.getLongitude());
                vehicle.setLatitude(data.vehicle.getLatitude());
                vehicle.setName(data.vehicle.getName());
                vehicle.setStakeId(data.vehicle.getStakeId());
                vehicle.setId(data.vehicle.getId());
                vehicle.setDirection(data.vehicle.getDirection());
                vehicle.setTimeStampStr(data.vehicle.getTimeStampStr());
                vehicle.setSource(data.vehicle.getSource()); // 传递source字段
                vehicle.setSpecialFlag(data.vehicle.getSpecialFlag()); // 传递specialFlag字段
                activeVehicles.add(vehicle);
                activeCount++;
            }
        }
        
        // 添加调试日志（每10次请求打印一次，避免日志过多）
        if (System.currentTimeMillis() % 10000 < 500) {
            System.out.println("getVehicles: 总车辆数=" + totalCount + ", 活跃车辆数=" + activeCount + ", 返回数量=" + activeVehicles.size());
            // 打印前3个车辆的信息用于调试
            int debugCount = 0;
            for (StartupRunner.VehicleData data : StartupRunner.VEHICLE_STORE.values()) {
                if (data.active && debugCount < 3) {
                    System.out.println("  车辆: " + data.vehicle.getName() + " (ID: " + data.vehicle.getId() + 
                        "), 位置: (" + data.vehicle.getLongitude() + ", " + data.vehicle.getLatitude() + 
                        "), 最后更新: " + (System.currentTimeMillis() - data.lastUpdate) + "ms前");
                    debugCount++;
                }
            }
        }
        
        return activeVehicles;
    }

    @GetMapping("/number")
    public Map<String, Integer> getVehicleNumbers() {
        Map<String, Integer> result = new HashMap<>();
        int total = 0;
        int direction1 = 0;
        int direction2 = 0;

        // 统计活跃车辆
        for (StartupRunner.VehicleData data : StartupRunner.VEHICLE_STORE.values()) {
            if (data.active) {
                total++;
                if (data.vehicle.getDirection() == 1) {
                    direction1++;
                } else if (data.vehicle.getDirection() == 2) {
                    direction2++;
                }
            }
        }

        result.put("total", total);
        result.put("direction1", direction1);
        result.put("direction2", direction2);

        return result;
    }

    @GetMapping("/vehicles")
    @ResponseBody
    public List<poiS> getByTimeSpatial() {
        try {
            return hbaseService.getByTimeSpatial();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 添加测试车辆数据（用于测试和演示）
     * 支持 GET 和 POST 请求
     */
    @RequestMapping(value = "/test/addVehicle", method = {RequestMethod.GET, RequestMethod.POST})
    public Map<String, Object> addTestVehicle(
            @RequestParam(required = false, defaultValue = "30.84627") double latitude,
            @RequestParam(required = false, defaultValue = "114.03636") double longitude,
            @RequestParam(required = false, defaultValue = "测试A00001") String plateNo,
            @RequestParam(required = false, defaultValue = "1") int direction) {
        
        poiS vehicle = new poiS();
        vehicle.setLongitude(longitude);
        vehicle.setLatitude(latitude);
        vehicle.setName(plateNo);
        vehicle.setId(System.currentTimeMillis());
        vehicle.setDirection(direction);
        vehicle.setStakeId("K1016+20");
        vehicle.setTimeStampStr(java.time.LocalDateTime.now().toString());

        // 添加到存储
        String key = "show_" + vehicle.getName() + "_" + vehicle.getId();
        StartupRunner.VehicleData data = new StartupRunner.VehicleData(vehicle);
        StartupRunner.VEHICLE_STORE.put(key, data);

        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("message", "测试车辆已添加");
        result.put("vehicle", vehicle);
        result.put("totalVehicles", StartupRunner.VEHICLE_STORE.size());
        return result;
    }

    /**
     * 获取当前存储的车辆数量统计（包括非活跃的）
     */
    @GetMapping("/test/stats")
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        int total = StartupRunner.VEHICLE_STORE.size();
        int active = 0;
        int inactive = 0;

        for (StartupRunner.VehicleData data : StartupRunner.VEHICLE_STORE.values()) {
            if (data.active) {
                active++;
            } else {
                inactive++;
            }
        }

        stats.put("total", total);
        stats.put("active", active);
        stats.put("inactive", inactive);
        stats.put("recentlySeenVehicles", StartupRunner.RECENTLY_SEEN_VEHICLES.size());
        return stats;
    }

    /**
     * 获取最近处理的车辆数据示例（用于调试）
     */
    @GetMapping("/test/debug")
    public Map<String, Object> getDebugInfo() {
        Map<String, Object> debug = new HashMap<>();
        debug.put("vehicleStoreSize", StartupRunner.VEHICLE_STORE.size());
        debug.put("recentlySeenVehiclesSize", StartupRunner.RECENTLY_SEEN_VEHICLES.size());
        
        // 获取前5个活跃车辆作为示例
        List<Map<String, Object>> sampleVehicles = new ArrayList<>();
        int count = 0;
        for (StartupRunner.VehicleData data : StartupRunner.VEHICLE_STORE.values()) {
            if (data.active && count < 5) {
                Map<String, Object> vehicleInfo = new HashMap<>();
                vehicleInfo.put("name", data.vehicle.getName());
                vehicleInfo.put("id", data.vehicle.getId());
                vehicleInfo.put("direction", data.vehicle.getDirection());
                vehicleInfo.put("longitude", data.vehicle.getLongitude());
                vehicleInfo.put("latitude", data.vehicle.getLatitude());
                vehicleInfo.put("stakeId", data.vehicle.getStakeId());
                vehicleInfo.put("lastUpdate", data.lastUpdate);
                vehicleInfo.put("active", data.active);
                sampleVehicles.add(vehicleInfo);
                count++;
            }
        }
        debug.put("sampleVehicles", sampleVehicles);
        
        return debug;
    }

    /**
     * 获取基站数据（位置和监测范围）
     */
    @GetMapping("/basestations")
    public List<BaseStationReader.BaseStation> getBaseStations() {
        try {
            return BaseStationReader.loadBaseStations("bsLocation_xg_0724.xlsx");
        } catch (IOException e) {
            log.error("读取基站数据失败", e);
            return new ArrayList<>();
        }
    }
}