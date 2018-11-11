package com.dd.edata.db;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author wangshupeng
 */
public final class DBServiceWithLogProxy extends AbstractDBServiceProxy {
    private static final Logger logger = LoggerFactory.getLogger(DBServiceWithLogProxy.class);
    private FileDBLog logFile;
    private File logDir;
    private AtomicLong txid = new AtomicLong(0);

    public DBServiceWithLogProxy(int sid, String logPath) {
        super(sid);
        this.logDir = new File(logPath);
        if (!this.logDir.exists()) {
            this.logDir.mkdirs();
        }
        logFile = new FileDBLog(sid, logDir);
    }

    @Override
    public void init(String pkg, ClassLoader cl, DataSource ds, boolean isCobar) {
        super.init(pkg, cl, ds, isCobar);
        this.recoverFromLog(cl);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        try {
            logFile.close();
        } catch (IOException e) {
            logger.error("close log file error!", e);
        }
    }

    private long writeDBLog(byte op, Object t) {
        long tx = txid.incrementAndGet();
        try {
            logFile.append(tx, op, t);
        } catch (Exception e) {
            logger.error("write data {} db log file error!", t, e);
        }
        return tx;
    }

    private void dbSyncSuccess(long tx) {
        try {
            logFile.append(tx, DBService.DB_TX_COMMIT);
        } catch (Exception e) {
            logger.error("write tx {} db log file error!", tx, e);
        }
    }

    private void recoverFromLog(ClassLoader cl) {
        Map<Long, Task> tasks = new LinkedHashMap<>();
        SimpleDateFormat sdf = new SimpleDateFormat(".yyyy-MM-dd-HH-mm-ss");
        String strDate = sdf.format(new Date());
        // data file
        String oldDataFileName = logDir.getAbsolutePath() + "/log." + sid + ".data";
        File dataFile = new File(oldDataFileName);
        File newDataFile = new File(oldDataFileName + strDate);
        dataFile.renameTo(newDataFile);
        try (Input dataIn = new Input(new BufferedInputStream(new FileInputStream(newDataFile)))) {
            Kryo kryo = new Kryo();
            kryo.setClassLoader(cl);
            long tx;
            while ((tx = dataIn.readLong()) != 0) {
                byte op = dataIn.readByte();
                if (op == DBService.DB_TX_COMMIT) {
                    tasks.remove(tx);
                } else {
                    tasks.put(tx, new Task(op, kryo.readClassAndObject(dataIn)));
                }
            }
        } catch (FileNotFoundException e) {
            logger.warn("server {} recover from log file error, not found file!!", sid);
        } catch (Exception e) {
            logger.error("server {} recover from log file error!", sid, e);
            return;
        }
        for (Task task : tasks.values()) {
            byte op = task.getOp();
            Object obj = task.getValue();
            switch (op) {
                case DBService.DB_INSERT:
                    insertAsync(null, null, obj);
                    break;
                case DBService.DB_INSERT_BATCH:
                    insertBatchAsync(null, null, (List<?>) obj);
                    break;
                case DBService.DB_UPDATE:
                    updateAsync(null, null, obj);
                    break;
                case DBService.DB_UPDATE_BATCH:
                    updateBatchAsync(null, null, (List<?>) obj);
                    break;
                case DBService.DB_UPDATE_WHERE:
                    OperateData data = (OperateData) obj;
                    updateAsync(null, null, data.clazz, data.name, data.value, data.wheres);
                    break;
                case DBService.DB_DELETE:
                    deleteAsync(null, null, obj);
                    break;
                case DBService.DB_DELETE_BATCH:
                    deleteBatchAsync(null, null, (List<?>) obj);
                    break;
                case DBService.DB_DELETE_WHERE:
                    data = (OperateData) obj;
                    this.deleteAsync(null, null, data.clazz, data.wheres);
                    break;
                case DBService.DB_TRUNCATE:
                    truncateAsync(null, null, (Class) obj);
                    break;
                default:
                    break;
            }
        }
        logger.info("[{}] server [{}] tasks recovered from log file", sid, tasks.size());
    }

    @Override
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        final long tx = writeDBLog(DBService.DB_DELETE, t);
        ExecutorService es = getExecutor(t.getClass());
        return execute(() -> {
            try {
                return dbService.delete(t);
            } catch (Exception e) {
                logger.error("sid {} deleteAsync error!", sid, e);
            } finally {
                dbSyncSuccess(tx);
            }
            return false;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<int[]> deleteBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        final long tx = writeDBLog(DBService.DB_DELETE_BATCH, objs);
        ExecutorService es = getExecutor(objs.get(0).getClass());
        return execute(() -> {
            try {
                return dbService.batchDelete(objs);
            } catch (Exception e) {
                logger.error("sid {} deleteBatchAsync error!", sid, e);
            } finally {
                dbSyncSuccess(tx);
            }
            return null;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, T t) {
        final long tx = writeDBLog(DBService.DB_UPDATE, t);
        ExecutorService es = getExecutor(t.getClass());
        return execute(() -> {
            try {
                return dbService.update(t);
            } catch (Exception e) {
                logger.error("sid {} updateAsync error!", sid, e);
            } finally {
                dbSyncSuccess(tx);
            }
            return 0;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<int[]> updateBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        final long tx = writeDBLog(DBService.DB_UPDATE_BATCH, objs);
        ExecutorService es = getExecutor(objs.get(0).getClass());
        return execute(() -> {
            try {
                return dbService.batchUpdate(objs);
            } catch (Exception e) {
                logger.error("sid {} updateBatchAsync error!", sid, e);
            } finally {
                dbSyncSuccess(tx);
            }
            return null;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Boolean> insertAsync(Consumer<Boolean> callback, Executor callbackExecutor, T t) {
        final long tx = writeDBLog(DBService.DB_INSERT, t);
        ExecutorService es = getExecutor(t.getClass());
        return execute(() -> {
            try {
                return dbService.insert(t);
            } catch (Exception e) {
                logger.error("sid {} addAsync error!", sid, e);
            } finally {
                dbSyncSuccess(tx);
            }
            return false;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<int[]> insertBatchAsync(Consumer<int[]> callback, Executor callbackExecutor, List<T> objs) {
        final long tx = writeDBLog(DBService.DB_INSERT_BATCH, objs);
        ExecutorService es = getExecutor(objs.get(0).getClass());
        return execute(() -> {
            try {
                return dbService.batchInsert(objs);
            } catch (Exception e) {
                logger.error("sid {} addBatchAsync error!", sid, e);
            } finally {
                dbSyncSuccess(tx);
            }
            return null;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Boolean> deleteAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz, DBWhere... wheres) {
        final long tx = writeDBLog(DBService.DB_DELETE_WHERE, new OperateData(clazz, wheres));
        ExecutorService es = getExecutor(clazz);
        return execute(() -> {
            try {
                return dbService.delete(clazz, wheres);
            } catch (Exception e) {
                logger.error("sid {} deleteAsync error!", sid, e);
            } finally {
                dbSyncSuccess(tx);
            }
            return false;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Integer> updateAsync(Consumer<Integer> callback, Executor callbackExecutor, Class<T> clazz, String name, Object value, DBWhere... wheres) {
        final long tx = writeDBLog(DBService.DB_UPDATE_WHERE, new OperateData(name, value, clazz, wheres));
        ExecutorService es = getExecutor(clazz);
        return execute(() -> {
            try {
                return dbService.update(clazz, name, value, wheres);
            } catch (Exception e) {
                logger.error("sid {} updateAsync error!", sid, e);
            } finally {
                dbSyncSuccess(tx);
            }
            return -1;
        }, es, callback, callbackExecutor);
    }

    @Override
    public <T> Future<Boolean> truncateAsync(Consumer<Boolean> callback, Executor callbackExecutor, Class<T> clazz) {
        final long tx = writeDBLog(DBService.DB_TRUNCATE, clazz);
        ExecutorService es = getExecutor(clazz);
        return execute(() -> {
            try {
                return dbService.truncate(clazz);
            } catch (Exception e) {
                logger.error("sid {} truncateAsync error!", sid, e);
            } finally {
                dbSyncSuccess(tx);
            }
            return false;
        }, es, callback, callbackExecutor);
    }

    static class OperateData {
        String name;
        Object value;
        Class<?> clazz;
        DBWhere[] wheres;

        OperateData() {
        }

        OperateData(String name, Object value, Class<?> clazz, DBWhere[] wheres) {
            this.name = name;
            this.value = value;
            this.clazz = clazz;
            this.wheres = wheres;
        }

        OperateData(Class<?> clazz, DBWhere[] wheres) {
            this.clazz = clazz;
            this.wheres = wheres;
        }
    }

    static class Task {
        byte op;
        Object value;

        public Task(byte op, Object value) {
            this.op = op;
            this.value = value;
        }

        public byte getOp() {
            return op;
        }

        public Object getValue() {
            return value;
        }
    }
}
