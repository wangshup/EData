package com.dd.edata.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * @author wangshupeng
 */
public final class DBServiceProxy extends AbstractDBServiceProxy {
    private static final Logger logger = LoggerFactory.getLogger(DBServiceProxy.class);

    public DBServiceProxy(int sid) {
        super(sid);
    }

    @Override
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        ExecutorService es = getExecutor(t.getClass());
        return execute(() -> {
            try {
                return dbService.delete(t);
            } catch (Exception e) {
                logger.error("sid {} deleteAsync error!", sid, e);
            }
            return false;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<int[]> deleteBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {

        ExecutorService es = getExecutor(objs.get(0).getClass());
        return execute(() -> {
            try {
                return dbService.batchDelete(objs);
            } catch (Exception e) {
                logger.error("sid {} deleteBatchAsync error!", sid, e);
            }
            return null;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, T t) {
        ExecutorService es = getExecutor(t.getClass());
        return execute(() -> {
            try {
                return dbService.update(t);
            } catch (Exception e) {
                logger.error("sid {} updateAsync error!", sid, e);
            }
            return 0;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<int[]> updateBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        ExecutorService es = getExecutor(objs.get(0).getClass());
        return execute(() -> {
            try {
                return dbService.batchUpdate(objs);
            } catch (Exception e) {
                logger.error("sid {} updateBatchAsync error!", sid, e);
            }
            return null;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Boolean> insertAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        ExecutorService es = getExecutor(t.getClass());
        return execute(() -> {
            try {
                return dbService.insert(t);
            } catch (Exception e) {
                logger.error("sid {} addAsync error!", sid, e);
            }
            return false;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<int[]> insertBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        ExecutorService es = getExecutor(objs.get(0).getClass());
        return execute(() -> {
            try {
                return dbService.batchInsert(objs);
            } catch (Exception e) {
                logger.error("sid {} addBatchAsync error!", sid, e);
            }
            return null;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Boolean> truncateAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz) {
        ExecutorService es = getExecutor(clazz);
        return execute(() -> {
            try {
                return dbService.truncate(clazz);
            } catch (Exception e) {
                logger.error("sid {} truncateAsync error!", sid, e);
            }
            return false;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres) {
        ExecutorService es = getExecutor(clazz);
        return execute(() -> {
            try {
                return dbService.delete(clazz, wheres);
            } catch (Exception e) {
                logger.error("sid {} deleteAsync error!", sid, e);
            }
            return false;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, Class<T> clazz, String name, Object value, DBWhere... wheres) {
        ExecutorService es = getExecutor(clazz);
        return execute(() -> {
            try {
                return dbService.update(clazz, name, value, wheres);
            } catch (Exception e) {
                logger.error("sid {} updateAsync error!", sid, e);
            }
            return -1;
        }, es, callback, callbackExecutor);
    }
}
