package com.dd.edata.db;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * @author wangshupeng
 */
public interface IDBProxy {

    int getSid();

    /**
     * 查询一个对象
     *
     * @param clazz   对象class
     * @param columns 只查询指定列
     * @param wheres  查询条件
     * @return
     * @throws Exception
     */
    <T> T select(Class<T> clazz, List<String> columns, DBWhere... wheres) throws Exception;


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
    <T> Future<T> selectAsync(Consumer<T> callback, Executor callbackExecutor, Class<T> clazz, List<String> columns, DBWhere... wheres);


    /**
     * 查询一组对象
     *
     * @param clazz   对象class
     * @param columns 只查询指定列
     * @param wheres  查询条件
     * @return
     * @throws Exception
     */
    <T> List<T> selectList(Class<T> clazz, List<String> columns, DBWhere... wheres) throws Exception;


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
    <T> Future<List<T>> selectListAsync(Consumer<List<T>> callback, Executor callbackExecutor, Class<T> clazz, List<String> columns, DBWhere... wheres);

    /**
     * 查询对象数量
     *
     * @param clazz  对象class
     * @param wheres 查询条件
     * @return
     * @throws Exception
     */
    <T> int count(Class<T> clazz, DBWhere... wheres) throws Exception;

    /**
     * 异步查询对象数量
     *
     * @param callback         查询回调接口
     * @param callbackExecutor 回调方法执行器
     * @param clazz            对象class
     * @param wheres           查询条件
     * @return
     */
    <T> Future<Integer> countAsync(Consumer<Integer> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres);

    <T> boolean delete(Class<T> clazz, DBWhere... wheres) throws Exception;

    <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres);

    /**
     * 删除一条数据
     *
     * @param t 待删除数据对象
     * @throws Exception
     */
    <T> boolean delete(T t) throws Exception;

    /**
     * 异步删除一条数据
     *
     * @param callback         删除回调接口
     * @param callbackExecutor 回调接口执行器
     * @param t                待删除数据对象
     * @return
     */
    <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t);

    /**
     * 批量删除一组数据
     *
     * @param objs 待删除的数据列表
     * @throws Exception
     */
    <T> int[] deleteBatch(List<T> objs) throws Exception;

    /**
     * 异步删除一组数据
     *
     * @param callback         删除回调接口
     * @param callbackExecutor 回调接口执行器
     * @param objs             待删除的数据列表
     * @return
     */
    <T> Future<int[]> deleteBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs);

    /**
     * 更新一条数据
     *
     * @param t 待更新数据
     * @throws Exception
     */
    <T> int update(T t) throws Exception;

    /**
     * 异步更新一条数据
     *
     * @param callback         更新数据回调接口
     * @param callbackExecutor 回调接口执行器
     * @param t                待更新数据
     * @return
     */
    <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, T t);

    <T> int update(Class<T> clazz, String name, Object value, DBWhere... wheres) throws Exception;

    <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, Class<T> clazz, String name, Object value, DBWhere... wheres);

    /**
     * 同步更新一组数据
     *
     * @param objs 待更新数据列表
     * @return
     * @throws Exception
     */
    <T> int[] updateBatch(List<T> objs) throws Exception;

    /**
     * 异步更新一组数据
     *
     * @param callback         更新回调接口
     * @param callbackExecutor 回调接口执行器
     * @param objs             待更新数据列表
     * @return
     */
    <T> Future<int[]> updateBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs);

    /**
     * 增加（插入）一条数据
     *
     * @param t 数据
     * @throws Exception
     */
    <T> boolean insert(T t) throws Exception;

    /**
     * 异步增加（插入）一条数据
     *
     * @param callback         插入回调接口
     * @param callbackExecutor 回调接口执行器
     * @param t                数据
     * @return
     */
    <T> Future<Boolean> insertAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t);

    /**
     * 增加（插入）一组数据
     *
     * @param objs 数据列表
     * @return
     * @throws Exception
     */
    <T> int[] insertBatch(List<T> objs) throws Exception;

    /**
     * 异步增加（插入）一组数据
     *
     * @param callback         插入数据回调接口
     * @param callbackExecutor 回调接口执行器
     * @param objs             数据列表
     * @return
     */
    <T> Future<int[]> insertBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs);

    /**
     * 增加（插入）一条数据
     *
     * @param t 数据
     * @throws Exception
     */
    <T> boolean replace(T t) throws Exception;

    /**
     * 异步增加（插入）一条数据
     *
     * @param callback         插入回调接口
     * @param callbackExecutor 回调接口执行器
     * @param t                数据
     * @return
     */
    <T> Future<Boolean> replaceAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t);

    /**
     * 增加（插入）一组数据
     *
     * @param objs 数据列表
     * @return
     * @throws Exception
     */
    <T> int[] replaceBatch(List<T> objs) throws Exception;

    /**
     * 异步增加（插入）一组数据
     *
     * @param callback         插入数据回调接口
     * @param callbackExecutor 回调接口执行器
     * @param objs             数据列表
     * @return
     */
    <T> Future<int[]> replaceBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs);

    /**
     * 删除表中所有数据（慎重，再慎重！！！）
     *
     * @param clazz 表映射的class
     * @throws Exception
     */
    <T> boolean truncate(Class<T> clazz) throws Exception;

    /**
     * 异步删除表中所有数据（慎重，再慎重！！！）
     *
     * @param callback         删除表回调接口
     * @param callbackExecutor 回调接口执行器
     * @param clazz            表映射的class
     * @return
     */
    <T> Future<Boolean> truncateAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz);
}
