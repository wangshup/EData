package com.dd.edata.db;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author wangshupeng
 */
public final class DBServiceReadonlyProxy extends AbstractDBServiceProxy {
    private final AtomicInteger idx = new AtomicInteger();

    public DBServiceReadonlyProxy(int sid) {
        super(sid);
    }

    @Override
    protected ExecutorService getExecutor(Class<?> clazz) {
        return executors[(executors.length - 1) & idx.getAndIncrement()];
    }

    @Override
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<int[]> deleteBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<int[]> updateBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<Boolean> insertAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<int[]> insertBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<Boolean> replaceAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<int[]> replaceBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<Boolean> truncateAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, Class<T> clazz, String name, Object value, DBWhere... wheres) {
        throw new UnsupportedOperationException();
    }
}
