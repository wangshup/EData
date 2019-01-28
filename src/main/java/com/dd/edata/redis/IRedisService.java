package com.dd.edata.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Tuple;

/**
 * redis 服务接口类
 * 
 * @author wangshupeng
 *
 */
public interface IRedisService {

    String set(String key, String value);

    String set(String key, String value, int cacheSeconds);

    String get(String key);

    void rpush(String key, String data);

    void rpush(String key, String data, int cacheSeconds);

    void lpush(String key, String data);

    void lpush(String key, String data, int cacheSeconds);

    String lpop(String key);

    String rpop(String key);

    void lrem(String key, List<String> values);

    List<String> lrange(String key);

    // hash
    void hmset(String key, Map<String, String> map);

    void hmset(String key, Map<String, String> map, int cacheSeconds);

    void hset(String key, String field, String value);

    void hset(String key, String field, String value, int cacheSeconds);

    Map<String, String> hgetAll(String key);

    List<Map<String, String>> hgetAllPipeline(List<String> keys);

    String hget(String key, String field);

    void sadd(String key, String[] value);

    // set
    void sadd(String key, String value);

    void srem(String key, String value);

    Set<String> smembers(String key);

    List<String> srandmember(String key, int count);

    // sorted set
    void zadd(String key, double score, String member);

    void zaddPipeline(String key, Map<String, Double> map);

    Long zcount(String key, double min, double max);

    Set<String> zrange(String key, long start, long end);

    void zrem(String key, String member);

    Set<String> zrangeByScore(String key, double min, double max, int offset, int count);

    Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count);

    Set<String> zrevrange(String key, long start, long end);

    Set<Tuple> zrangeWithScores(String key, long start, long end);

    Set<Tuple> zrevrangeWithScores(String key, long start, long end);

    Long zrank(String key, String member);

    Long zrevrank(String key, String member);

    /**
     * @param key
     * @param member
     * @return 不存在时，返回null
     */
    Double zscore(String key, String member);

    long zcard(String key);

    long ttl(String key);

    long expire(String key, int seconds);

    long del(String key);

    boolean exists(String key);
}
