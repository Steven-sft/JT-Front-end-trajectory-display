//package com.ljj.toshow.demos.controller;
//
//import com.alibaba.fastjson2.JSON;
//import com.ljj.toshow.demos.pojo.PTData;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.redis.core.RedisTemplate;
//import org.springframework.web.bind.annotation.GetMapping;
//import org.springframework.web.bind.annotation.RestController;
//
//import java.util.List;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//@RestController
//public class PointController {
//    @Autowired
//    private RedisTemplate<String, String> redisTemplate;
//
//    @GetMapping("/points")
//    public List<PTData.PathPoint> getPoints() {
//        Set<String> keys = redisTemplate.keys("vehicle:*");
//        return keys.stream()
//                .map(key -> JSON.parseObject(redisTemplate.opsForValue().get(key), PTData.PathPoint.class))
//                .collect(Collectors.toList());
//    }
//}