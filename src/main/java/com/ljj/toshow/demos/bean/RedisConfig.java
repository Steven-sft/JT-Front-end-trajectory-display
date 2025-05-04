package com.ljj.toshow.demos.bean;

import com.ljj.toshow.demos.pojo.TrafficEventUtils;
import com.ljj.toshow.demos.pojo.poiS;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.io.IOException;

@Configuration
public class RedisConfig {

  @Bean
  public RedisTemplate<String, poiS> redisTemplate(RedisConnectionFactory connectionFactory) {
    RedisTemplate<String, poiS> template = new RedisTemplate<>();
    template.setConnectionFactory(connectionFactory);
    template.setKeySerializer(new StringRedisSerializer());
    template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
    return template;
  }
  @Bean
    @Qualifier("mileageConverter1")
    public TrafficEventUtils.MileageConverter mileageConverter1(@Value("${converter1.file}") String filePath) throws IOException, IOException {
        return new TrafficEventUtils.MileageConverter(filePath);
    }

    @Bean
    @Qualifier("mileageConverter2")
    public TrafficEventUtils.MileageConverter mileageConverter2(@Value("${converter2.file}") String filePath) throws IOException {
        return new TrafficEventUtils.MileageConverter(filePath);
    }

}
