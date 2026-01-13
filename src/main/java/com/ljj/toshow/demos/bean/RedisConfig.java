package com.ljj.toshow.demos.bean;

import com.ljj.toshow.demos.pojo.TrafficEventUtils;
import com.ljj.toshow.demos.pojo.poiS;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

@Configuration
public class RedisConfig {
@Value("${spring.redis.host}")
    private String redisHost;

    @Value("${spring.redis.port}")
    private int redisPort;

    @Value("${spring.redis.password}")
    private String redisPassword;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        // 单机配置
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(redisHost);
        config.setPort(redisPort);
        config.setPassword(redisPassword); // 设置密码

        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
            .commandTimeout(Duration.ofSeconds(2))
            .build();

        return new LettuceConnectionFactory(config, clientConfig);
    }
  @Bean
  public RedisTemplate<String, poiS> redisTemplate() {
    RedisTemplate<String, poiS> template = new RedisTemplate<>();
    template.setConnectionFactory(redisConnectionFactory());
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
