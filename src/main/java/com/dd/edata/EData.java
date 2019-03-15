package com.dd.edata;

import com.dd.edata.db.*;
import com.dd.edata.redis.IRedisService;
import com.dd.edata.redis.RedisPool;
import com.dd.edata.redis.RedisService;
import com.dd.edata.utils.FileMonitor;
import com.dd.edata.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Tuple;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Edata服务类
 *
 * @author wangshupeng
 */

/**
 * Edata服务类
 *
 * @author wangshupeng
 */
public final class EData {
    private static final Logger logger = LoggerFactory.getLogger(EData.class);
    private static Map<Integer, EData> eDatas = new HashMap<>();
    private static ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor();
    private AtomicInteger refCount = new AtomicInteger(0);
    private IDBProxy dbProxy;
    private volatile IRedisService redisService;
    private int sid;

    private EData(int sid, String logPath) {
        this.sid = sid;
        dbProxy = new DBServiceWithLogProxy(sid, logPath);
    }

    private EData(int sid) {
        this.sid = sid;
        dbProxy = new DBServiceProxy(sid);
    }

    private EData(int sid, boolean readOnly) {
        this.sid = sid;
        dbProxy = new DBServiceReadonlyProxy(sid);
    }

    /**
     * 启动EData服务
     *
     * @param sid      服务器ID
     * @param pkg      数据库实体类所在的Java包路径
     * @param cl       pkg的类加载器
     * @param propFile edata配置文件，默认：edata.properties
     * @return EData
     */
    public static synchronized EData start(int sid, String pkg, ClassLoader cl, String propFile) {
        boolean bMonitor = !eDatas.containsKey(sid);
        EData eData = start(sid, pkg, cl, Util.getProperties(propFile));
        if (bMonitor) eData.startPropFileMonitor(propFile);
        return eData;
    }

    public static synchronized EData start(int sid, String pkg, ClassLoader cl, Properties props) {
        return start(sid, pkg, cl, props, false);
    }

    public static synchronized EData start(int sid, String pkg, ClassLoader cl, Properties props, boolean readOnly) {
        EData eData = eDatas.get(sid);
        if (eData != null) {
            logger.warn("The Edata server {} has been started!!", sid);
        } else {
            if (readOnly) {
                eData = new EData(sid, true);
            } else {
                boolean bWriteLog = Boolean.parseBoolean(props.getProperty("db.log", "true"));
                if (bWriteLog) {
                    eData = new EData(sid, props.getProperty("db.log.dir", "dblogs"));
                } else {
                    eData = new EData(sid);
                }
            }
            eData.init(sid, pkg, cl, props);
            eDatas.put(sid, eData);
        }
        return eData.retain();
    }

    public static synchronized EData start(int sid, String pkg, ClassLoader cl) {
        return start(sid, pkg, cl, "edata.properties");
    }

    private void init(int sid, String pkg, ClassLoader cl, Properties props) {
        ClassLoader classLoader = cl != null ? cl : Thread.currentThread().getContextClassLoader();
        ((AbstractDBServiceProxy) dbProxy).init(pkg, classLoader, props);
        redisService = new RedisService(new RedisPool(sid, props));
    }

    private void propertiesReload(Properties props) {
        ((AbstractDBServiceProxy) dbProxy).propertiesReload(props);
        IRedisService newRs = new RedisService(new RedisPool(sid, props));
        IRedisService oldRs = redisService;
        redisService = newRs;
        if (oldRs != null) {
            ((RedisService) oldRs).shutdown();
        }
    }

    private void startPropFileMonitor(String file) {
        FileMonitor monitor = new FileMonitor(false);
        monitor.addFile(file);
        monitor.addListener(list -> {
            for (FileMonitor.MonitoredFile f : list) {
                if (f.getFile().getName().equalsIgnoreCase(file)) {
                    propertiesReload(Util.getProperties(file));
                    logger.info("EData properties file [{}] reload success!!!", file);
                    break;
                }
            }
        });
        schedule.scheduleAtFixedRate(monitor, 120, 30, TimeUnit.SECONDS);
    }

    /**
     * 关闭EData服务
     */
    public synchronized void shutdown() {
        if (release() <= 0) {
            eDatas.remove(sid);
            if (dbProxy != null) {
                ((AbstractDBServiceProxy) dbProxy).shutdown();
            }
            if (redisService != null) {
                ((RedisService) redisService).shutdown();
            }
        }
    }

    private EData retain() {
        refCount.incrementAndGet();
        return this;
    }

    private int release() {
        return refCount.decrementAndGet();
    }

    /**
     * 查询一个对象
     *
     * @param clazz  对象class
     * @param wheres 查询条件
     * @return
     * @throws Exception
     */
    public <T> T select(Class<T> clazz, DBWhere... wheres) throws Exception {
        return select(clazz, null, wheres);
    }

    /**
     * 查询一个对象
     *
     * @param clazz   对象class
     * @param columns 只查询指定列
     * @param wheres  查询条件
     * @return
     * @throws Exception
     */
    public <T> T select(Class<T> clazz, List<String> columns, DBWhere... wheres) throws Exception {
        return dbProxy.select(clazz, columns, wheres);
    }

    /**
     * 异步查询一个对象
     *
     * @param clazz  对象class
     * @param wheres 查询条件
     * @return
     */
    public <T> Future<T> selectAsync(Class<T> clazz, DBWhere... wheres) {
        return selectAsync(clazz, null, wheres);
    }

    /**
     * 异步查询一个对象
     *
     * @param clazz   对象class
     * @param columns 只查询指定列
     * @param wheres  查询条件
     * @return
     */
    public <T> Future<T> selectAsync(Class<T> clazz, List<String> columns, DBWhere... wheres) {
        return selectAsync(null, null, clazz, columns, wheres);
    }

    /**
     * 异步查询一个对象
     *
     * @param callback 查询回调接口
     * @param clazz    对象class
     * @param wheres   查询条件
     * @return
     */
    public <T> Future<T> selectAsync(Consumer<T> callback, Class<T> clazz, DBWhere... wheres) {
        return selectAsync(callback, clazz, null, wheres);
    }

    /**
     * 异步查询一个对象
     *
     * @param callback 查询回调接口
     * @param clazz    对象class
     * @param columns  只查询指定列
     * @param wheres   查询条件
     * @return
     */
    public <T> Future<T> selectAsync(Consumer<T> callback, Class<T> clazz, List<String> columns, DBWhere... wheres) {
        return selectAsync(callback, null, clazz, columns, wheres);
    }

    /**
     * 异步查询一个对象
     *
     * @param callback         查询回调接口
     * @param callbackExecutor 回调方法执行器
     * @param clazz            对象class
     * @param wheres           查询条件
     * @return
     */
    public <T> Future<T> selectAsync(Consumer<T> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres) {
        return selectAsync(callback, callbackExecutor, clazz, null, wheres);
    }

    /**
     * 异步查询一个对象
     *
     * @param callback         查询回调接口
     * @param callbackExecutor 回调方法执行器
     * @param clazz            对象class
     * @param columns          只查询指定列
     * @param wheres           查询条件
     * @return
     */
    public <T> Future<T> selectAsync(Consumer<T> callback, Executor callbackExecutor, Class<T> clazz, List<String> columns, DBWhere... wheres) {
        return dbProxy.selectAsync(callback, callbackExecutor, clazz, columns, wheres);
    }

    /**
     * 查询一组对象
     *
     * @param clazz  对象class
     * @param wheres 查询条件
     * @return
     * @throws Exception
     */
    public <T> List<T> selectList(Class<T> clazz, DBWhere... wheres) throws Exception {
        return selectList(clazz, null, wheres);
    }

    /**
     * 查询一组对象
     *
     * @param clazz   对象class
     * @param columns 只查询指定列
     * @param wheres  查询条件
     * @return
     * @throws Exception
     */
    public <T> List<T> selectList(Class<T> clazz, List<String> columns, DBWhere... wheres) throws Exception {
        return dbProxy.selectList(clazz, columns, wheres);
    }

    /**
     * 异步查询一组对象
     *
     * @param clazz  对象class
     * @param wheres 查询条件
     * @return
     */
    public <T> Future<List<T>> selectListAsync(Class<T> clazz, DBWhere... wheres) {
        return selectListAsync(null, null, clazz, wheres);
    }

    /**
     * 异步查询一组对象
     *
     * @param clazz   对象class
     * @param columns 只查询指定列
     * @param wheres  查询条件
     * @return
     */
    public <T> Future<List<T>> selectListAsync(Class<T> clazz, List<String> columns, DBWhere... wheres) {
        return selectListAsync(null, null, clazz, columns, wheres);
    }

    /**
     * 异步查询一组对象
     *
     * @param callback 查询回调接口
     * @param clazz    对象class
     * @param wheres   查询条件
     * @return
     */
    public <T> Future<List<T>> selectListAsync(Consumer<List<T>> callback, Class<T> clazz, DBWhere... wheres) {
        return selectListAsync(callback, null, clazz, wheres);
    }

    /**
     * 异步查询一组对象
     *
     * @param callback 查询回调接口
     * @param clazz    对象class
     * @param columns  只查询指定列
     * @param wheres   查询条件
     * @return
     */
    public <T> Future<List<T>> selectListAsync(Consumer<List<T>> callback, Class<T> clazz, List<String> columns, DBWhere... wheres) {
        return selectListAsync(callback, null, clazz, columns, wheres);
    }

    /**
     * 异步查询一组对象
     *
     * @param callback         查询回调接口
     * @param callbackExecutor 回调方法执行器
     * @param clazz            对象class
     * @param wheres           查询条件
     * @return
     */
    public <T> Future<List<T>> selectListAsync(Consumer<List<T>> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres) {
        return selectListAsync(callback, callbackExecutor, clazz, null, wheres);
    }

    /**
     * 异步查询一组对象
     *
     * @param callback         查询回调接口
     * @param callbackExecutor 回调方法执行器
     * @param clazz            对象class
     * @param columns          只查询指定列
     * @param wheres           查询条件
     * @return
     */
    public <T> Future<List<T>> selectListAsync(Consumer<List<T>> callback, Executor callbackExecutor, Class<T> clazz, List<String> columns, DBWhere... wheres) {
        return dbProxy.selectListAsync(callback, callbackExecutor, clazz, columns, wheres);
    }

    /**
     * 查询对象数量
     *
     * @param clazz  对象class
     * @param wheres 查询条件
     * @return
     * @throws Exception
     */
    public <T> int count(Class<T> clazz, DBWhere... wheres) throws Exception {
        return dbProxy.count(clazz, wheres);
    }

    /**
     * 异步查询对象数量
     *
     * @param callback         查询回调接口
     * @param callbackExecutor 回调方法执行器
     * @param clazz            对象class
     * @param wheres           查询条件
     * @return
     */
    public <T> Future<Integer> countAsync(Consumer<Integer> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres) {
        return dbProxy.countAsync(callback, callbackExecutor, clazz, wheres);
    }

    /**
     * 异步查询对象数量
     *
     * @param callback 查询回调接口
     * @param clazz    对象class
     * @param wheres   查询条件
     * @return
     */
    public <T> Future<Integer> countAsync(Consumer<Integer> callback, Class<T> clazz, DBWhere... wheres) {
        return countAsync(callback, null, clazz, wheres);
    }

    /**
     * 异步查询对象数量
     *
     * @param clazz  对象class
     * @param wheres 查询条件
     * @return
     */
    public <T> Future<Integer> countAsync(Class<T> clazz, DBWhere... wheres) {
        return countAsync(null, null, clazz, wheres);
    }

    /**
     * 删除一条数据
     *
     * @param t 待删除数据对象
     * @throws Exception
     */
    public <T> boolean delete(T t) throws Exception {
        return dbProxy.delete(t);
    }

    /**
     * 根据where同步删除记录
     *
     * @param clazz  记录类
     * @param wheres where条件
     * @return
     * @throws Exception
     */
    public <T> boolean delete(Class<T> clazz, DBWhere... wheres) throws Exception {
        return dbProxy.delete(clazz, wheres);
    }

    /**
     * 异步删除一条数据
     *
     * @param t 待删除数据对象
     * @return
     */
    public <T> Future<Boolean> deleteAsync(T t) {
        return deleteAsync(null, null, t);
    }

    /**
     * 异步删除一条数据
     *
     * @param callback 删除回调接口
     * @param t        待删除数据对象
     * @return
     */
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, T t) {
        return deleteAsync(callback, null, t);
    }

    /**
     * 根据where异步删除记录
     *
     * @param clazz  记录类
     * @param wheres where条件
     * @return
     */
    public <T> Future<Boolean> deleteAsync(Class<T> clazz, DBWhere... wheres) {
        return deleteAsync(null, null, clazz, wheres);
    }

    /**
     * 根据where异步删除记录
     *
     * @param callback 删除回调
     * @param clazz    记录类
     * @param wheres   where条件
     * @return
     */
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Class<T> clazz, DBWhere... wheres) {
        return deleteAsync(callback, null, clazz, wheres);
    }

    /**
     * 异步删除一条数据
     *
     * @param callback         删除回调接口
     * @param callbackExecutor 回调接口执行器
     * @param t                待删除数据对象
     * @return
     */
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        return dbProxy.deleteAsync(callback, callbackExecutor, t);
    }

    /**
     * 根据where异步删除记录
     *
     * @param callback         删除回调
     * @param callbackExecutor 回调执行器
     * @param clazz            记录类
     * @param wheres           where条件
     * @return
     */
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres) {
        return dbProxy.deleteAsync(callback, callbackExecutor, clazz, wheres);
    }

    /**
     * 批量删除一组数据
     *
     * @param objs 待删除的数据列表
     * @throws Exception
     */
    public <T> int[] deleteBatch(List<T> objs) throws Exception {
        if (objs == null || objs.isEmpty()) {
            return null;
        }
        return dbProxy.deleteBatch(objs);
    }

    /**
     * 异步删除一组数据
     *
     * @param objs 待删除的数据列表
     * @return
     */
    public <T> Future<int[]> deleteBatchAsync(List<T> objs) {
        return deleteBatchAsync(null, null, objs);
    }

    /**
     * 异步删除一组数据
     *
     * @param callback 删除回调接口
     * @param objs     待删除的数据列表
     * @return
     */
    public <T> Future<int[]> deleteBatchAsync(Consumer<int[]> callback, List<T> objs) {
        return deleteBatchAsync(callback, null, objs);
    }

    /**
     * 异步删除一组数据
     *
     * @param callback         删除回调接口
     * @param callbackExecutor 回调接口执行器
     * @param objs             待删除的数据列表
     * @return
     */
    public <T> Future<int[]> deleteBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        if (objs == null || objs.isEmpty()) {
            return null;
        }
        return dbProxy.deleteBatchAsync(callback, callbackExecutor, objs);
    }

    /**
     * 更新一条数据
     *
     * @param t 待更新数据
     * @throws Exception
     */
    public <T> int update(T t) throws Exception {
        return dbProxy.update(t);
    }

    /**
     * 根据where同步更新一个字段
     *
     * @param clazz  记录类
     * @param name   更新的字段名（类中的变量名）
     * @param value  更新的值
     * @param wheres where条件
     * @return
     * @throws Exception
     */
    public <T> int update(Class<T> clazz, String name, Object value, DBWhere... wheres) throws Exception {
        return dbProxy.update(clazz, name, value, wheres);
    }

    /**
     * 异步更新一条数据
     *
     * @param t 待更新数据
     * @return
     */
    public <T> Future<Integer> updateAsync(T t) {
        return updateAsync(null, null, t);
    }

    /**
     * 异步更新一条数据
     *
     * @param callback 更新数据回调接口
     * @param t        待更新数据
     * @return
     */
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, T t) {
        return updateAsync(callback, null, t);
    }

    /**
     * 异步更新一条数据
     *
     * @param callback         更新数据回调接口
     * @param callbackExecutor 回调接口执行器
     * @param t                待更新数据
     * @return
     */
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, T t) {
        return dbProxy.updateAsync(callback, callbackExecutor, t);
    }

    /**
     * 根据where异步更新一个字段
     *
     * @param clazz  记录类
     * @param name   更新的字段名（类中的变量名）
     * @param value  更新的值
     * @param wheres where条件
     * @return
     */
    public <T> Future<Integer> updateAsync(Class<T> clazz, String name, Object value, DBWhere... wheres) {
        return updateAsync(null, null, clazz, name, value, wheres);
    }

    /**
     * 根据where异步更新一个字段
     *
     * @param callback 更新回调
     * @param clazz    记录类
     * @param name     更新的字段名（类中的变量名）
     * @param value    更新的值
     * @param wheres   where条件
     * @return
     */
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, Class<T> clazz, String name, Object value, DBWhere... wheres) {
        return updateAsync(callback, null, clazz, name, value, wheres);
    }

    /**
     * 根据where异步更新一个字段
     *
     * @param callback         更新回调
     * @param callbackExecutor 回调执行器
     * @param clazz            记录类
     * @param name             更新的字段名（类中的变量名）
     * @param value            更新的值
     * @param wheres           where条件
     * @return
     */
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, Class<T> clazz, String name, Object value, DBWhere... wheres) {
        return dbProxy.updateAsync(callback, callbackExecutor, clazz, name, value, wheres);
    }

    /**
     * 同步更新一组数据
     *
     * @param objs 待更新数据列表
     * @return
     * @throws Exception
     */
    public <T> int[] updateBatch(List<T> objs) throws Exception {
        if (objs == null || objs.isEmpty()) {
            return null;
        }
        return dbProxy.updateBatch(objs);
    }

    /**
     * 异步更新一组数据
     *
     * @param objs 待更新数据列表
     * @return
     */
    public <T> Future<int[]> updateBatchAsync(List<T> objs) {
        return updateBatchAsync(null, null, objs);
    }

    /**
     * 异步更新一组数据
     *
     * @param callback 更新回调接口
     * @param objs     待更新数据列表
     * @return
     */
    public <T> Future<int[]> updateBatchAsync(Consumer<int[]> callback, List<T> objs) {
        return updateBatchAsync(callback, null, objs);
    }

    /**
     * 异步更新一组数据
     *
     * @param callback         更新回调接口
     * @param callbackExecutor 回调接口执行器
     * @param objs             待更新数据列表
     * @return
     */
    public <T> Future<int[]> updateBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        if (objs == null || objs.isEmpty()) {
            return null;
        }
        return dbProxy.updateBatchAsync(callback, callbackExecutor, objs);
    }

    /**
     * 增加（插入）一条数据
     *
     * @param t 数据
     * @throws Exception
     */
    public <T> boolean insert(T t) throws Exception {
        return dbProxy.insert(t);
    }

    /**
     * 异步增加（插入）一条数据
     *
     * @param t 数据
     * @return
     */
    public <T> Future<Boolean> insertAsync(T t) {
        return insertAsync(null, null, t);
    }

    /**
     * 异步增加（插入）一条数据
     *
     * @param callback 插入回调接口
     * @param t        数据
     * @return
     */
    public <T> Future<Boolean> insertAsync(Consumer<Boolean> callback, T t) {
        return insertAsync(callback, null, t);
    }

    /**
     * 异步增加（插入）一条数据
     *
     * @param callback         插入回调接口
     * @param callbackExecutor 回调接口执行器
     * @param t                数据
     * @return
     */
    public <T> Future<Boolean> insertAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        return dbProxy.insertAsync(callback, callbackExecutor, t);
    }

    /**
     * 增加（插入）一组数据
     *
     * @param objs 数据列表
     * @return
     * @throws Exception
     */
    public <T> int[] insertBatch(List<T> objs) throws Exception {
        if (objs == null || objs.isEmpty()) {
            return null;
        }
        return dbProxy.insertBatch(objs);
    }

    /**
     * 异步增加（插入）一组数据
     *
     * @param objs 数据列表
     * @return
     */
    public <T> Future<int[]> insertBatchAsync(List<T> objs) {
        return insertBatchAsync(null, null, objs);
    }

    /**
     * 异步增加（插入）一组数据
     *
     * @param callback 插入数据回调接口
     * @param objs     数据列表
     * @return
     */
    public <T> Future<int[]> insertBatchAsync(Consumer<int[]> callback, List<T> objs) {
        return insertBatchAsync(callback, null, objs);
    }

    /**
     * 异步增加（插入）一组数据
     *
     * @param callback         插入数据回调接口
     * @param callbackExecutor 回调接口执行器
     * @param objs             数据列表
     * @return
     */
    public <T> Future<int[]> insertBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        if (objs == null || objs.isEmpty()) {
            return null;
        }
        return dbProxy.insertBatchAsync(callback, callbackExecutor, objs);
    }

    /**
     * 增加（插入或替换）一条数据
     *
     * @param t 数据
     * @throws Exception
     */
    public <T> boolean replace(T t) throws Exception {
        return dbProxy.replace(t);
    }

    /**
     * 异步增加（插入或替换）一条数据
     *
     * @param t 数据
     * @return
     */
    public <T> Future<Boolean> replaceAsync(T t) {
        return replaceAsync(null, null, t);
    }

    /**
     * 异步增加（插入或替换）一条数据
     *
     * @param callback 插入回调接口
     * @param t        数据
     * @return
     */
    public <T> Future<Boolean> replaceAsync(Consumer<Boolean> callback, T t) {
        return replaceAsync(callback, null, t);
    }

    /**
     * 异步增加（插入或替换）一条数据
     *
     * @param callback         插入回调接口
     * @param callbackExecutor 回调接口执行器
     * @param t                数据
     * @return
     */
    public <T> Future<Boolean> replaceAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        return dbProxy.replaceAsync(callback, callbackExecutor, t);
    }

    /**
     * 增加（插入或替换）一组数据
     *
     * @param objs 数据列表
     * @return
     * @throws Exception
     */
    public <T> int[] replaceBatch(List<T> objs) throws Exception {
        if (objs == null || objs.isEmpty()) {
            return null;
        }
        return dbProxy.replaceBatch(objs);
    }

    /**
     * 异步增加（插入或替换）一组数据
     *
     * @param objs 数据列表
     * @return
     */
    public <T> Future<int[]> replaceBatchAsync(List<T> objs) {
        return replaceBatchAsync(null, null, objs);
    }

    /**
     * 异步增加（插入或替换）一组数据
     *
     * @param callback 插入数据回调接口
     * @param objs     数据列表
     * @return
     */
    public <T> Future<int[]> replaceBatchAsync(Consumer<int[]> callback, List<T> objs) {
        return replaceBatchAsync(callback, null, objs);
    }

    /**
     * 异步增加（插入或替换）一组数据
     *
     * @param callback         插入数据回调接口
     * @param callbackExecutor 回调接口执行器
     * @param objs             数据列表
     * @return
     */
    public <T> Future<int[]> replaceBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        if (objs == null || objs.isEmpty()) {
            return null;
        }
        return dbProxy.replaceBatchAsync(callback, callbackExecutor, objs);
    }

    /**
     * 删除表中所有数据（慎重，再慎重！！！）
     *
     * @param clazz 表映射的class
     * @throws Exception
     */
    public <T> boolean truncate(Class<T> clazz) throws Exception {
        return dbProxy.truncate(clazz);
    }

    /**
     * 异步删除表中所有数据（慎重，再慎重！！！）
     *
     * @param clazz 表映射的class
     * @return
     */
    public <T> Future<Boolean> truncateAsync(Class<T> clazz) {
        return truncateAsync(null, null, clazz);
    }

    /**
     * 异步删除表中所有数据（慎重，再慎重！！！）
     *
     * @param callback 删除表回调接口
     * @param clazz    表映射的class
     * @return
     */
    public <T> Future<Boolean> truncateAsync(Consumer<Boolean> callback, Class<T> clazz) {
        return truncateAsync(callback, null, clazz);
    }

    /**
     * 异步删除表中所有数据（慎重，再慎重！！！）
     *
     * @param callback         删除表回调接口
     * @param callbackExecutor 回调接口执行器
     * @param clazz            表映射的class
     * @return
     */
    public <T> Future<Boolean> truncateAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz) {
        return dbProxy.truncateAsync(callback, callbackExecutor, clazz);
    }

    ////////////////////// redis client functions//////////////////////////////
    public String set(String key, String value) {
        return redisService.set(key, value);
    }

    public String set(String key, String value, int cacheSeconds) {
        return redisService.set(key, value, cacheSeconds);
    }

    public String get(String key) {
        return redisService.get(key);
    }

    public void rpush(String key, String data) {
        redisService.rpush(key, data);
    }

    public void rpush(String key, String data, int cacheSeconds) {
        redisService.rpush(key, data, cacheSeconds);
    }

    public void lpush(String key, String data) {
        redisService.lpush(key, data);
    }

    public void lpush(String key, String data, int cacheSeconds) {
        redisService.lpush(key, data, cacheSeconds);
    }

    public String lpop(String key) {
        return redisService.lpop(key);
    }

    public String rpop(String key) {
        return redisService.rpop(key);
    }

    public void lrem(String key, List<String> values) {
        redisService.lrem(key, values);
    }

    public List<String> lrange(String key) {
        return redisService.lrange(key);
    }

    // hash
    public void hmset(String key, Map<String, String> map) {
        redisService.hmset(key, map);
    }

    public void hmset(String key, Map<String, String> map, int cacheSeconds) {
        redisService.hmset(key, map, cacheSeconds);
    }

    public void hset(String key, String field, String value) {
        redisService.hset(key, field, value);
    }

    public void hset(String key, String field, String value, int cacheSeconds) {
        redisService.hset(key, field, value, cacheSeconds);
    }

    public Map<String, String> hgetAll(String key) {
        return redisService.hgetAll(key);
    }

    public List<Map<String, String>> hgetAllPipeline(List<String> keys) {
        return redisService.hgetAllPipeline(keys);
    }

    public String hget(String key, String field) {
        return redisService.hget(key, field);
    }

    public void sadd(String key, String[] value) {
        redisService.sadd(key, value);
    }

    // set
    public void sadd(String key, String value) {
        redisService.sadd(key, value);
    }

    public void srem(String key, String value) {
        redisService.srem(key, value);
    }

    public Set<String> smembers(String key) {
        return redisService.smembers(key);
    }

    public List<String> srandmember(String key, int count) {
        return redisService.srandmember(key, count);
    }

    // sorted set
    public void zadd(String key, double score, String member) {
        redisService.zadd(key, score, member);
    }

    public void zaddPipeline(String key, Map<String, Double> map) {
        redisService.zaddPipeline(key, map);
    }

    public Long zcount(String key, double min, double max) {
        return redisService.zcount(key, min, max);
    }

    public Set<String> zrange(String key, long start, long end) {
        return redisService.zrange(key, start, end);
    }

    public void zrem(String key, String member) {
        redisService.zrem(key, member);
    }

    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return redisService.zrangeByScore(key, min, max, offset, count);
    }

    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return redisService.zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<String> zrevrange(String key, long start, long end) {
        return redisService.zrevrange(key, start, end);
    }

    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return redisService.zrangeWithScores(key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return redisService.zrevrangeWithScores(key, start, end);
    }

    public Long zrank(String key, String member) {
        return redisService.zrank(key, member);
    }

    public Long zrevrank(String key, String member) {
        return redisService.zrevrank(key, member);
    }

    /**
     * @param key
     * @param member
     * @return 不存在时，返回null
     */
    public Double zscore(String key, String member) {
        return redisService.zscore(key, member);
    }

    public long zcard(String key) {
        return redisService.zcard(key);
    }

    public long ttl(String key) {
        return redisService.ttl(key);
    }

    public long expire(String key, int seconds) {
        return redisService.expire(key, seconds);
    }

    public long del(String key) {
        return redisService.del(key);
    }

    public boolean exists(String key) {
        return redisService.exists(key);
    }
}

