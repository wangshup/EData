package com.dd.edata.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author wangshupeng
 */
public abstract class AbstractDBServiceProxy implements IDBProxy {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDBServiceProxy.class);
    private static final int WORK_QUEUE_SIZE = 2048;
    protected final ExecutorService[] executors;
    protected DBService dbService;
    protected int sid;


    public AbstractDBServiceProxy(int sid) {
        this.sid = sid;
        int size = Runtime.getRuntime().availableProcessors();
        size = ceilingPowerOfTwo(size << 1);
        ExecutorService[] ess = new ExecutorService[size];
        for (int i = 0; i < size; ++i) {
            ess[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(WORK_QUEUE_SIZE, true),
                    new ThreadFactoryImpl("EData-Thread", sid, i),
                    (r, e) -> logger.error("Thread task's queue full, current size {}, max size {} ", e.getQueue().size(), WORK_QUEUE_SIZE));
        }
        executors = ess;
    }

    private static DataSource createDataSource(Properties props) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(props.getProperty("db.driver.class", "com.mysql.jdbc.Driver"));
        config.setJdbcUrl(props.getProperty("db.url"));
        config.setUsername(props.getProperty("db.user"));
        config.setPassword(props.getProperty("db.password"));
        config.setAutoCommit(Boolean.parseBoolean(props.getProperty("db.autocommit", "true")));
        config.setConnectionTestQuery("select 1");
        config.setConnectionTimeout(Long.parseLong(props.getProperty("db.connectionTimeout", "1000")));
        config.setMaximumPoolSize(Integer.parseInt(props.getProperty("db.maxPoolSize", "32")));
        config.setMinimumIdle(Integer.parseInt(props.getProperty("db.minIdle", "4")));
        config.setLeakDetectionThreshold(Long.parseLong(props.getProperty("db.leakDetectionThreshold", "30000")));
        return new HikariDataSource(config);
    }

    private static final int ceilingPowerOfTwo(int v) {
        if (v <= 0) return 1;
        int i = v - 1;
        i |= i >>> 1;
        i |= i >>> 2;
        i |= i >>> 4;
        i |= i >>> 8;
        i |= i >>> 16;
        return i + 1;
    }

    public void init(String pkg, ClassLoader cl, DataSource ds, boolean isCobar) {
        dbService = new DBService(this, pkg, cl, ds, isCobar);
    }

    public void init(String pkg, ClassLoader cl, Properties props) {
        init(pkg, cl, createDataSource(props), Boolean.parseBoolean(props.getProperty("db.cobar", "false")));
    }

    public void shutdown() {
        for (ExecutorService es : executors) {
            try {
                es.shutdown();
                es.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.error("executor {} shutdown error!", es, e);
            }
        }
    }

    protected <T> Future<T> execute(Supplier<T> supplier, Executor executor, Consumer<? super T> callback, Executor callbackExecutor) {
        CompletableFuture<T> future = CompletableFuture.supplyAsync(supplier, executor);
        if (callback != null) {
            if (callbackExecutor != null) future.thenAcceptAsync(callback, callbackExecutor);
            else future.thenAcceptAsync(callback);
        }
        return future;
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
    @Override
    public <T> T select(Class<T> clazz, List<String> columns, DBWhere... wheres) throws Exception {
        return selectAsync(null, null, clazz, columns, wheres).get();
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
    @Override
    public <T> Future<T> selectAsync(Consumer<T> callback, Executor callbackExecutor, Class<T> clazz, List<String> columns, DBWhere... wheres) {
        ExecutorService es = getExecutor(clazz);
        return execute(() -> {
            try {
                return dbService.select(clazz, columns, wheres);
            } catch (Exception e) {
                logger.error("sid {} getAsync {{}} error!", sid, clazz.getName(), e);
            }
            return null;
        }, es, callback, callbackExecutor);
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
    @Override
    public <T> List<T> selectList(Class<T> clazz, List<String> columns, DBWhere... wheres) throws Exception {
        return selectListAsync(null, null, clazz, columns, wheres).get();
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
    @Override
    public <T> Future<List<T>> selectListAsync(Consumer<List<T>> callback, Executor callbackExecutor, Class<T> clazz, List<String> columns, DBWhere... wheres) {
        ExecutorService es = getExecutor(clazz);
        return execute(() -> {
            try {
                return dbService.selectList(clazz, columns, wheres);
            } catch (Exception e) {
                logger.error("sid {} getListAsync {{}} error!", sid, clazz.getName(), e);
            }
            return null;
        }, es, callback, callbackExecutor);
    }

    /**
     * 查询对象数量
     *
     * @param clazz  对象class
     * @param wheres 查询条件
     * @return
     * @throws Exception
     */
    @Override
    public <T> int count(Class<T> clazz, DBWhere... wheres) throws Exception {
        return countAsync(null, null, clazz, wheres).get();
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
    @Override
    public <T> Future<Integer> countAsync(Consumer<Integer> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres) {
        ExecutorService es = getExecutor(clazz);
        return execute(() -> {
            try {
                return dbService.count(clazz, wheres);
            } catch (Exception e) {
                logger.error("sid {} countAsync {{}} error!", sid, clazz.getName(), e);
            }
            return null;
        }, es, callback, callbackExecutor);
    }

    /**
     * 删除一条数据
     *
     * @param t 待删除数据对象
     * @throws Exception
     */
    @Override
    public <T> boolean delete(T t) throws Exception {
        return deleteAsync(null, null, t).get();
    }


    /**
     * 批量删除一组数据
     *
     * @param objs 待删除的数据列表
     * @throws Exception
     */
    @Override
    public <T> int[] deleteBatch(List<T> objs) throws Exception {
        return deleteBatchAsync(null, null, objs).get();
    }


    /**
     * 更新一条数据
     *
     * @param t 待更新数据
     * @throws Exception
     */
    @Override
    public <T> int update(T t) throws Exception {
        return updateAsync(null, null, t).get();
    }

    /**
     * 同步更新一组数据
     *
     * @param objs 待更新数据列表
     * @return
     * @throws Exception
     */
    @Override
    public <T> int[] updateBatch(List<T> objs) throws Exception {
        return updateBatchAsync(null, null, objs).get();
    }


    /**
     * 增加（插入）一条数据
     *
     * @param t 数据
     * @throws Exception
     */
    @Override
    public <T> boolean insert(T t) throws Exception {
        return insertAsync(null, null, t).get();
    }


    /**
     * 增加（插入）一组数据
     *
     * @param objs 数据列表
     * @return
     * @throws Exception
     */
    @Override
    public <T> int[] insertBatch(List<T> objs) throws Exception {
        return insertBatchAsync(null, null, objs).get();
    }


    /**
     * 删除表中所有数据（慎重，再慎重！！！）
     *
     * @param clazz 表映射的class
     * @throws Exception
     */
    @Override
    public <T> boolean truncate(Class<T> clazz) throws Exception {
        return truncateAsync(null, null, clazz).get();
    }

    @Override
    public <T> boolean delete(Class<T> clazz, DBWhere... wheres) throws Exception {
        return deleteAsync(null, null, clazz, wheres).get();
    }

    @Override
    public <T> int update(Class<T> clazz, String name, Object value, DBWhere... wheres) throws Exception {
        return updateAsync(null, null, clazz, name, value, wheres).get();
    }

    final ExecutorService getExecutor(Class<?> clazz) {
        return executors[(executors.length - 1) & clazz.hashCode()];
    }

    static class ThreadFactoryImpl implements ThreadFactory {
        private final String namePrefix;
        private final int sid;
        private final int id;

        ThreadFactoryImpl(final String namePrefix, final int sid, final int id) {
            this.namePrefix = namePrefix;
            this.sid = sid;
            this.id = id;
        }

        @Override
        public Thread newThread(final Runnable target) {
            return new Thread(target, this.namePrefix + "[" + this.sid + "-" + this.id + "]");
        }
    }
}
