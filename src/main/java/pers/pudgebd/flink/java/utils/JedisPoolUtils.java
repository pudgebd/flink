package pers.pudgebd.flink.java.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolUtils.class);


    private static volatile JedisPool jedisPool = null;

    synchronized public static Jedis getJedis() {
        if (jedisPool == null) {
            synchronized (JedisPoolUtils.class) {
                if (jedisPool == null) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(1000);
                    config.setMaxIdle(100);
                    jedisPool = new JedisPool(config,
                            "cdh174", 1131, 5000,
                            "abc123", 1, "");
                    return jedisPool.getResource();
                } else {
                    return jedisPool.getResource();
                }
            }
        } else {
            return jedisPool.getResource();
        }
    }

    public static void close(Jedis jedis) {
        if (jedis == null || jedisPool == null) {
            return;
        }
        try {
            jedis.close();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

}
