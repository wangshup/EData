package com.dd.edata.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.Tuple;

public class RedisService implements IRedisService {
    private static final Logger logger = LoggerFactory.getLogger(RedisService.class);
    private RedisPool redisPool;

    public RedisService(RedisPool redisPool) {
        this.redisPool = redisPool;
    }

    public void shutdown() {
        redisPool.shutdown();
    }

    @Override
    public String set(String key, String value) {
        return set(key, value, 0);
    }

    @Override
    public String set(String key, String value, int cacheSeconds) {
        String lastVal = null;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            lastVal = jedis.getSet(key, value);
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
        } catch (Exception e) {
            logger.error("failed: set key:{}, value:{}", key, value, e);
        }
        return lastVal;
    }

    @Override
    public String get(String key) {
        String value = null;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            if (jedis.exists(key)) {
                value = jedis.get(key);
            }
        } catch (Exception e) {
            logger.error("failed: get key:{}", key, e);
        }
        return value;
    }

    @Override
    public void rpush(String key, String data) {
        rpush(key, data, 0);
    }

    @Override
    public void rpush(String key, String data, int cacheSeconds) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            jedis.rpush(key, data);
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
        } catch (Exception e) {
            logger.error("failed: rpush key:{},data:{}", key, data, e);
        }
    }

    @Override
    public void lpush(String key, String data) {
        lpush(key, data, 0);
    }

    @Override
    public void lpush(String key, String data, int cacheSeconds) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            jedis.lpush(key, data);
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
        } catch (Exception e) {
            logger.error("failed: lpush key:{},data:{}", key, data, e);
        }
    }

    @Override
    public String lpop(String key) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.lpop(key);
        } catch (Exception e) {
        }
        return null;
    }

    @Override
    public String rpop(String key) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.rpop(key);
        } catch (Exception e) {
        }
        return null;
    }

    @Override
    public void lrem(String key, List<String> values) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            ShardedJedisPipeline pipeline = jedis.pipelined();
            if (values != null && !values.isEmpty()) {
                for (String val : values) {
                    pipeline.lrem(key, 0, val);
                }
            }
            pipeline.sync();
        } catch (Exception e) {
            logger.error("failed: lrem key:{},value: {}", key, values, e);
        }
    }

    @Override
    public List<String> lrange(String key) {
        List<String> list = null;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            if (jedis.exists(key)) {
                list = jedis.lrange(key, 0, -1);
            }
        } catch (Exception e) {
            logger.error("failed: lrange key:{},list:{}", key, list, e);
        }
        return list;
    }

    @Override
    public void hmset(String key, Map<String, String> map) {
        hmset(key, map, 0);
    }

    @Override
    public void hmset(String key, Map<String, String> map, int cacheSeconds) {
        if (map != null && map.size() > 0) {
            try (ShardedJedis jedis = redisPool.getJedis()) {
                jedis.hmset(key, map);
                if (cacheSeconds != 0)
                    jedis.expire(key, cacheSeconds);
            } catch (Exception e) {
                logger.error("failed: hmset key:{}, map:{}", key, map, e);
            }
        }
    }

    @Override
    public void hset(String key, String field, String value) {
        hset(key, field, value, 0);
    }

    @Override
    public void hset(String key, String field, String value, int cacheSeconds) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            if (jedis != null) {
                jedis.hset(key, field, value);
                if (cacheSeconds != 0)
                    jedis.expire(key, cacheSeconds);
            }
        } catch (Exception e) {
            logger.error("failed: hset key:{},field:{},value:{}", key, field, value, e);
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        Map<String, String> hashMap = null;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            hashMap = jedis.hgetAll(key);
        } catch (Exception e) {
            logger.error("failed: hgetAll key:{}", key, e);
        }
        return hashMap;
    }

    @Override
    public List<Map<String, String>> hgetAllPipeline(List<String> keys) {
        List<Map<String, String>> list = Lists.newArrayList();
        try (ShardedJedis jedis = redisPool.getJedis()) {
            List<Response<Map<String, String>>> responseList = Lists.newArrayList();
            ShardedJedisPipeline pipeLine = jedis.pipelined();
            for (String key : keys) {
                responseList.add(pipeLine.hgetAll(key));
            }
            pipeLine.sync();
            for (Response<Map<String, String>> response : responseList) {
                list.add(response.get());
            }
        } catch (Exception e) {
            logger.error("failed: hgetAllPipeline key:{}", keys, e);
        }
        return list;
    }

    @Override
    public String hget(String key, String field) {
        String value = null;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            if (jedis != null) {
                if (jedis.exists(key)) {
                    value = jedis.hget(key, field);
                }
            }
        } catch (Exception e) {
            logger.error("failed: hget key:{},field:{}", key, field, e);
        }
        return value;
    }

    @Override
    public void sadd(String key, String[] value) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            jedis.sadd(key, value);
        } catch (Exception e) {
            logger.error("failed: sadd key:{},value:{}", key, value, e);
        }
    }

    @Override
    public void sadd(String key, String value) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            jedis.sadd(key, value);
        } catch (Exception e) {
            logger.error("failed: sadd key:{},value:{}", key, value, e);
        }
    }

    @Override
    public void srem(String key, String value) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            jedis.srem(key, value);
        } catch (Exception e) {
            logger.error("failed: srem key:{},value:{}", key, value, e);
        }
    }

    @Override
    public Set<String> smembers(String key) {
        Set<String> set = null;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            if (jedis.exists(key)) {
                set = jedis.smembers(key);
            }
        } catch (Exception e) {
            logger.error("failed: smembers key:{}", key, e);
        }
        return set;
    }

    @Override
    public List<String> srandmember(String key, int count) {
        List<String> set = null;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            if (jedis.exists(key)) {
                set = jedis.srandmember(key, count);
            }
        } catch (Exception e) {
            logger.error("failed: smembers key:{}", key, e);
        }
        return set;
    }

    // sorted set
    @Override
    public void zadd(String key, double score, String member) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            jedis.zadd(key, score, member);
        } catch (Exception e) {
            logger.error("failed: zadd key:{},score:{},member:{}", key, score, member, e);
        }
    }

    @Override
    public void zaddPipeline(String key, Map<String, Double> map) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            ShardedJedisPipeline pipeline = jedis.pipelined();
            for (Entry<String, Double> entry : map.entrySet()) {
                pipeline.zadd(key, entry.getValue(), entry.getKey());
            }
            pipeline.sync();
        } catch (Exception e) {
            logger.error("failed: zaddPipeline key:{} ", key, e);
        }
    }

    @Override
    public Long zcount(String key, double min, double max) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zcount(key, min, max);
        } catch (Exception e) {
            logger.error("failed: zcount key:{},min:{},max:{}", key, min, max, e);
        }
        return 0L;
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zrange(key, start, end);
        } catch (Exception e) {
            logger.error("failed: zrange key:{},start:{},end:{}", key, start, end, e);
        }
        return null;
    }

    @Override
    public void zrem(String key, String member) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            jedis.zrem(key, member);
        } catch (Exception e) {
            logger.error("failed: zrem key:{},member:{}", key, member, e);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            logger.error("failed: zrangeByScore key:{},min:{},max:{},offset:{},count:{}", key, min, max, offset, count,
                    e);
        }
        return null;
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zrevrangeByScore(key, max, min, offset, count);
        } catch (Exception e) {
            logger.error("failed: zrevrangeByScore key:{},max:{},min:{},offset:{},count:{}", key, max, min, offset,
                    count, e);
        }
        return null;
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zrevrange(key, start, end);
        } catch (Exception e) {
            logger.error("failed: zrevrange key:{},start:{},end:{}", key, start, end, e);
        }
        return null;
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zrangeWithScores(key, start, end);
        } catch (Exception e) {
            logger.error("failed: zrangeWithScores key:{},start:{},end:{}", key, start, end, e);
        }
        return null;
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zrevrangeWithScores(key, start, end);
        } catch (Exception e) {
            logger.error("failed: zrevrangeWithScores key:{},start:{},end:{}", key, start, end, e);
        }
        return null;
    }

    @Override
    public Long zrank(String key, String member) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zrank(key, member);
        } catch (Exception e) {
            logger.error("failed: zrank key:{},member:{}", key, member, e);
        }
        return -1L;
    }

    @Override
    public Long zrevrank(String key, String member) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zrevrank(key, member);
        } catch (Exception e) {
            logger.error("failed: zrevrank key:{},member:{}", key, member, e);
        }
        return -1L;
    }

    /**
     * @param key
     * @param member
     * @return 不存在时，返回null
     */
    @Override
    public Double zscore(String key, String member) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zscore(key, member);
        } catch (Exception e) {
            logger.error("failed: zscore key:{},member:{}", key, member, e);
        }
        return -1D;
    }

    @Override
    public long zcard(String key) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.zcard(key);
        } catch (Exception e) {
            logger.error("failed: zcard key:{}", key, e);
        }
        return 0;
    }

    @Override
    public long ttl(String key) {
        long result = 0;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.ttl(key);
        } catch (Exception e) {
            logger.error("failed: ttl {}", key, e);
        }
        return result;
    }

    @Override
    public long expire(String key, int seconds) {
        long result = 0;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            logger.error("failed: expire {}", key, e);
        }
        return result;
    }

    @Override
    public long del(String key) {
        long result = 0;
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.del(key);
        } catch (Exception e) {
            logger.error("failed: del {}", key, e);
        }
        return result;
    }

    @Override
    public boolean exists(String key) {
        try (ShardedJedis jedis = redisPool.getJedis()) {
            return jedis.exists(key);
        } catch (Exception e) {
            logger.error("failed: exists {}", key, e);
        }
        return false;
    }
}
