package com.dd.edata.utils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileMonitor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FileMonitor.class);

    private Map<String, MonitoredFile> monitoredFileMap = new ConcurrentHashMap<>();
    private Set<FileListener> listeners = new HashSet<>();
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private boolean dispatchByGroup = false;

    public FileMonitor(boolean dispatchByGroup) {
        this.dispatchByGroup = dispatchByGroup;
    }

    public FileMonitor(File file, boolean dispatchByGroup, FileListener listener) {
        this.dispatchByGroup = dispatchByGroup;
        this.addFile(file);
        this.addListener(listener);
    }

    public FileMonitor() {
    }

    public void addFile(File file) {
        addFile(file, MonitoredFile.DEFAULT_GROUP);
    }

    public void addFile(File file, String group, String parentFolder) {
        if (!monitoredFileMap.containsKey(file.getPath())) {
            monitoredFileMap.put(file.getPath(), new MonitoredFile(file, group, parentFolder));
        }
    }

    public void addFile(File file, String group) {
        addFile(file, group, "");
    }

    public void addFile(String path) {
        addFile(path, MonitoredFile.DEFAULT_GROUP);
    }

    public void addFile(String path, String group) {
        addFile(new File(path), group);
    }

    public void removeFile(File file) {
        monitoredFileMap.remove(file.getPath(), file);
    }

    public void removeFile(String file) {
        monitoredFileMap.remove(file);
    }

    public void addFolder(String path, String ext, boolean recursive, String group) {
        File dir = new File(path);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("provide path not exist or is not a valid directory");
        }
        Collection<File> props = FileUtils.listFiles(dir, new String[]{ext}, recursive);
        for (File f : props) {
            addFile(f, group, path);
        }
    }

    public void addFolder(String path, String ext, boolean recursive) {
        addFolder(path, ext, recursive, MonitoredFile.DEFAULT_GROUP);
    }

    public void addListener(FileListener listener) {
        readWriteLock.writeLock().lock();
        try {
            listeners.add(listener);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void removeListener(FileListener listener) {
        readWriteLock.writeLock().lock();
        try {
            listeners.remove(listener);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private void dispatchEvent(Collection<MonitoredFile> fileList) {
        for (FileListener listener : listeners) {
            try {
                listener.onChange(fileList);
            } catch (Throwable t) {
                logger.error("dispatch file change event fail for file {} on listener {}", fileList.size(), listener, t);
            }
        }
    }

    public Set<String> getMonitoredFiles() {
        return Collections.unmodifiableSet(monitoredFileMap.keySet());
    }

    public Collection<MonitoredFile> getMonitored() {
        return Collections.unmodifiableCollection(monitoredFileMap.values());
    }

    public List<MonitoredFile> getMonitordFileList() {
        return new ArrayList<>(monitoredFileMap.values());
    }

    @Override
    public void run() {
        if (monitoredFileMap.isEmpty()) {
            return;
        }
        try {
            batchDispatch();
        } catch (Throwable t) {
            logger.error("dispatch file change event fail", t);
        }
    }

    private void batchDispatch() {
        if (!readWriteLock.readLock().tryLock()) {
            return;
        }
        try {
            Map<String, List<MonitoredFile>> changedListMap = new HashMap<>();
            List<MonitoredFile> changedList = new ArrayList<>();
            for (MonitoredFile file : monitoredFileMap.values()) {
                if (file.isModified()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("file {} changed", file.getFile().getPath());
                    }
                    List<MonitoredFile> fileList = changedListMap.computeIfAbsent(file.getGroup(), (k) -> new ArrayList<>());
                    fileList.add(file);
                    changedList.add(file);
                    file.update();
                }
            }
            if (!changedListMap.isEmpty()) {
                if (dispatchByGroup) {
                    changedListMap.values().forEach(v -> dispatchEvent(v));
                } else {
                    dispatchEvent(changedList);
                }
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public int size() {
        return monitoredFileMap.size();
    }

    public static class MonitoredFile {
        public static final String DEFAULT_GROUP = "def";
        private File file;
        private long lastModified = -1;
        private String group;
        private String parent;

        MonitoredFile(File file) {
            this(file, DEFAULT_GROUP);
        }

        public MonitoredFile(File file, String group) {
            this(file, group, "");

        }

        public MonitoredFile(File file, String group, String parent) {
            this.file = file;
            this.group = group;
            this.parent = parent;
            update();
        }

        public String getGroup() {
            if (group == null || group.isEmpty()) {
                return DEFAULT_GROUP;
            }
            return group;
        }

        public MonitoredFile(String fileName) {
            this(new File(fileName));
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getParent() {
            return parent;
        }

        public void setParent(String parent) {
            this.parent = parent;
        }

        public File getFile() {
            return file;
        }

        public void update() {
            this.lastModified = file.lastModified();
        }

        public boolean isModified() {
            return this.lastModified == -1 || this.lastModified < file.lastModified();
        }

        @Override
        public String toString() {
            return file.getName();
        }
    }

    public interface FileListener {
        void onChange(Collection<MonitoredFile> changedList);
    }
}
