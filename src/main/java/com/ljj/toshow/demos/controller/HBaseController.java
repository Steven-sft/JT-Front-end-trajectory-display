package com.ljj.toshow.demos.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ljj.toshow.demos.pojo.*;
import com.ljj.toshow.demos.service.HBaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;

import static com.ljj.toshow.demos.bean.StartupRunner.redisTemplate;

@Slf4j
@RestController
public class HBaseController {

int a=0;
    @GetMapping("/points")
    public List<poiS> getVehicles() {
        List<poiS> vehicles = new ArrayList<>();

        // 获取所有车辆 Key（需优化，生产环境建议使用 SCAN）
        Set<String> keys = redisTemplate.keys("v*");
//        System.out.println(keys);
if(a==0&&keys.size()>10000){a++;
    System.out.println(10000);}else if(a==1&&keys.size()>30000){a++;
    System.out.println(30000);}else if(a==2&&keys.size()>50000){a++;
    System.out.println(50000);}else if(a==3&&keys.size()>70000){a++;
    System.out.println(70000);}else if(a==4&&keys.size()>90000){a++;
    System.out.println(90000);}else if(a==5&&keys.size()>110000){a++;
    System.out.println(110000);}else if(a==6&&keys.size()>130000){a++;
    System.out.println(130000);}else if(a==7&&keys.size()>150000){a++;
    System.out.println(150000);}else if(a==8&&keys.size()>170000){a++;
    System.out.println(170000);}else if(a==9&&keys.size()>190000){a++;
    System.out.println(190000);}else if(a==10&&keys.size()>200000){a++;
    System.out.println(200000);}
        for (String key : keys) {
//            System.out.println("key:"+key);
            poiS vehicle = redisTemplate.opsForValue().get(key);
            vehicles.add(vehicle);
        }
        return vehicles;
    }

@GetMapping("/number")
public Map<String, Integer> getVehicleNumbers() {
    Map<String, Integer> result = new HashMap<>();

    // 获取所有车辆Key
    Set<String> keys = redisTemplate.keys("v*");

    // 历史车辆总数
    int total = keys.size();

    // 方向统计
    int direction1 = 0;
    int direction2 = 0;

    for (String key : keys) {
        poiS vehicle = redisTemplate.opsForValue().get(key);
        if (vehicle != null) {
            if (vehicle.getDirection() == 1) {
                direction1++;
            } else if (vehicle.getDirection() == 2) {
                direction2++;
            }
        }
    }

    result.put("total", total);
    result.put("direction1", direction1);
    result.put("direction2", direction2);

    return result;
}
    @Autowired
    private HBaseService hbaseService;
//      @RequestMapping(value = "/getByTimeSpatial", method = RequestMethod.GET)
      @GetMapping("/vehicles")
      @ResponseBody
      //@RequestParam("tableName") String tableName,
    public List<poiS> getByTimeSpatial() {
        List<poiS> res;
          try {
              res=hbaseService.getByTimeSpatial();
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
          return res;
      }
}

//车流量   桩号，经纬度，起止时间，平均车速，交通饱和度
