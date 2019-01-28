package com.dd.edata.db;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 数据库日志文件
 *
 * @author wangshupeng
 */
public class FileDBLog {
    private static long forceSyncTime = 1 * 1000;
    private FilePadding filePadding = new FilePadding();
    private volatile FileOutputStream fos = null;
    private volatile Output out = null;
    private File logDir;
    private long lastForceSyncTime = 0;
    private int sid;
    private Kryo kryo = new Kryo();

    public FileDBLog(int sid, File logDir) {
        this.logDir = logDir;
        this.sid = sid;
    }

    public synchronized void close() throws IOException {
        if (fos != null) {
            fos.getChannel().force(false);
        }
        if (out != null) {
            out.close();
        }
        if (fos != null) {
            fos.close();
        }
    }

    public synchronized boolean append(long txid, byte op, Object obj) throws IOException {
        if (out == null) {
            fos = new FileOutputStream(new File(logDir, ("log." + sid + ".data")));
            out = new Output(fos);
            filePadding.setCurrentSize(fos.getChannel().position());
        }
        filePadding.padFile(fos.getChannel());
        out.writeLong(txid);
        out.writeByte(op);
        if (obj != null) {
            kryo.writeClassAndObject(out, obj);
        }
        commit();
        return true;
    }

    public boolean append(long txid, byte op) throws IOException {
        return append(txid, op, null);
    }

    private void commit() throws IOException {
        if (out != null) {
            out.flush();
        }
        fos.flush();
        long now = System.currentTimeMillis();
        if (now - lastForceSyncTime > forceSyncTime) {
            fos.getChannel().force(false);
            lastForceSyncTime = now;
        }
    }
}
