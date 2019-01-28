package com.dd.edata.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * redis 服务类
 * 
 * @author wangshupeng
 *
 */
public class RedisPool {
    private static final Logger logger = LoggerFactory.getLogger(RedisPool.class);
    private ShardedJedisPool shardedJedisPool;

    public RedisPool(int sid, Properties props) {
        if (!props.containsKey("redis.ip")) {
            logger.info("Zone[{}] properties not contains redis config!!!!", sid);
        } else {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(Integer.parseInt(props.getProperty("redis.pool.maxActive")));
            poolConfig.setMaxIdle(Integer.parseInt(props.getProperty("redis.pool.maxIdle")));
            poolConfig.setMaxWaitMillis(Integer.parseInt(props.getProperty("redis.pool.maxWait")));
            poolConfig.setTestOnBorrow(Boolean.parseBoolean(props.getProperty("redis.pool.testOnBorrow")));
            poolConfig.setTestOnReturn(Boolean.parseBoolean(props.getProperty("redis.pool.testOnReturn")));
            String[] ips = props.getProperty("redis.ip").split(";");
            String[] ports = props.getProperty("redis.port").split(";");
            List<JedisShardInfo> shards = new ArrayList<>();
            for (int i = 0; i < ips.length; ++i) {
                shards.add(new JedisShardInfo(ips[i], Integer.parseInt(ports[i])));
            }
            shardedJedisPool = new ShardedJedisPool(poolConfig, shards);
            logger.info("Zone[{}] shared jedis pool to {} : {} created success!!!", sid, props.getProperty("redis.ip"),
                    props.getProperty("redis.port"));
        }
    }

    public ShardedJedis getJedis() {
        return shardedJedisPool.getResource();
    }

    public void shutdown() {
        if (shardedJedisPool != null)
            shardedJedisPool.close();
    }
}
