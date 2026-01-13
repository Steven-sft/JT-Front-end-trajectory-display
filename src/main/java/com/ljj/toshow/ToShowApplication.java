package com.ljj.toshow;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;


@SpringBootApplication
@EnableCaching
public class ToShowApplication {
//   public static final ConcurrentHashMap<String, poiS> resMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        SpringApplication.run(ToShowApplication.class, args);
    }

}