package com.dd.edata.utils;

import com.dd.edata.EData;
import com.dd.edata.db.DBDataType;
import com.dd.edata.db.annotation.Column;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.net.JarURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class Util {
    private static final Logger logger = LoggerFactory.getLogger(EData.class);

    private static final Gson gson = new Gson();
    private static final ThreadLocal<SimpleDateFormat> sdfHolder = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    public static String formatDate(Date d) {
        return sdfHolder.get().format(d);
    }

    /**
     * json字符串转成对象
     * 
     * @param str
     * @param type
     * @return
     */
    public static <T> T fromJson(String str, Type type) {
        return gson.fromJson(str, type);
    }

    public static <T> List<T> fromJsonToList(String json, Class<T> clazz) {
        List<T> lst = new ArrayList<T>();

        JsonArray array = new JsonParser().parse(json).getAsJsonArray();

        for (final JsonElement elem : array) {
            lst.add(gson.fromJson(elem, clazz));
        }

        return lst;
    }

    /**
     * json字符串转成对象
     * 
     * @param str
     * @param type
     * @return
     */
    public static <T> T fromJson(String str, Class<T> type) {
        return gson.fromJson(str, type);
    }

    /**
     * 对象转换成json字符串
     * 
     * @param obj
     * @return
     */
    public static String toJson(Object obj) {
        return gson.toJson(obj);
    }

    public static String getColName(Field field) {
        Column colann = field.getAnnotation(Column.class);
        if (colann == null)
            return null;
        return (colann.name() != null && !colann.name().equals("")) ? colann.name() : field.getName();
    }

    public static String getColType(Field field) {
        Column colann = field.getAnnotation(Column.class);
        if (colann == null)
            return null;
        return colann.type() != null && !colann.type().equals("") ? colann.type()
                : DBDataType.TYPE_MAP.get(field.getType());
    }

    public static String getColTypeWithLength(Field field) {
        Column ann = field.getAnnotation(Column.class);
        StringBuffer sb = new StringBuffer();
        String colType = Util.getColType(field);
        sb.append(colType);
        int colLen = ann.len();
        if (colLen != 0) {
            if (colType.equalsIgnoreCase("float") || colType.equalsIgnoreCase("double")
                    || colType.equalsIgnoreCase("decimal")) {
                sb.append("(").append(ann.len()).append(",").append(ann.precision()).append(")");
            } else {
                sb.append("(").append(ann.len()).append(")");
            }
        }
        return sb.toString().toLowerCase();
    }

    public static List<Class<?>> getClassList(String pkgName, boolean isRecursive,
            Class<? extends Annotation> annotation, ClassLoader cl) {
        List<Class<?>> classList = new ArrayList<Class<?>>();
        ClassLoader loader = cl;// Thread.currentThread().getContextClassLoader();
        try {
            // 按文件的形式去查找
            String strFile = pkgName.replaceAll("\\.", "/");
            Enumeration<URL> urls = loader.getResources(strFile);
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url != null) {
                    String protocol = url.getProtocol();
                    String pkgPath = url.getPath();
                    if ("file".equals(protocol)) {
                        // 本地自己可见的代码
                        findClassName(classList, pkgName, pkgPath, isRecursive, annotation, cl);
                    } else if ("jar".equals(protocol)) {
                        // 引用第三方jar的代码
                        findClassName(classList, pkgName, url, isRecursive, annotation, cl);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return classList;
    }

    public static void findClassName(List<Class<?>> clazzList, String pkgName, String pkgPath, boolean isRecursive,
            Class<? extends Annotation> annotation, ClassLoader cl) {
        if (clazzList == null) {
            return;
        }
        File[] files = filterClassFiles(pkgPath);// 过滤出.class文件及文件夹
        if (files != null) {
            for (File f : files) {
                String fileName = f.getName();
                if (f.isFile()) {
                    // .class 文件的情况
                    String clazzName = getClassName(pkgName, fileName);
                    addClassName(clazzList, clazzName, annotation, cl);
                } else {
                    // 文件夹的情况
                    if (isRecursive) {
                        // 需要继续查找该文件夹/包名下的类
                        String subPkgName = pkgName + "." + fileName;
                        String subPkgPath = pkgPath + "/" + fileName;
                        findClassName(clazzList, subPkgName, subPkgPath, true, annotation, cl);
                    }
                }
            }
        }
    }

    /**
     * 第三方Jar类库的引用。<br/>
     * 
     * @throws IOException
     */
    public static void findClassName(List<Class<?>> clazzList, String pkgName, URL url, boolean isRecursive,
            Class<? extends Annotation> annotation, ClassLoader cl) throws IOException {
        JarURLConnection jarURLConnection = (JarURLConnection) url.openConnection();
        JarFile jarFile = jarURLConnection.getJarFile();
        Enumeration<JarEntry> jarEntries = jarFile.entries();
        while (jarEntries.hasMoreElements()) {
            JarEntry jarEntry = jarEntries.nextElement();
            // 类似：sun/security/internal/interfaces/TlsMasterSecret.class
            String jarEntryName = jarEntry.getName();
            String clazzName = jarEntryName.replace("/", ".");
            int endIndex = clazzName.lastIndexOf(".");
            String prefix = null;
            if (endIndex > 0) {
                String prefix_name = clazzName.substring(0, endIndex);
                endIndex = prefix_name.lastIndexOf(".");
                if (endIndex > 0) {
                    prefix = prefix_name.substring(0, endIndex);
                }
            }
            if (prefix != null && jarEntryName.endsWith(".class")) {
                if (prefix.equals(pkgName)) {
                    addClassName(clazzList, clazzName, annotation, cl);
                } else if (isRecursive && prefix.startsWith(pkgName)) {
                    // 遍历子包名：子类
                    addClassName(clazzList, clazzName, annotation, cl);
                }
            }
        }
    }

    private static File[] filterClassFiles(String pkgPath) {
        if (pkgPath == null) {
            return null;
        }
        // 接收 .class 文件 或 类文件夹
        return new File(pkgPath).listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return (file.isFile() && file.getName().endsWith(".class")) || file.isDirectory();
            }
        });
    }

    private static String getClassName(String pkgName, String fileName) {
        int endIndex = fileName.lastIndexOf(".");
        String clazz = null;
        if (endIndex >= 0) {
            clazz = fileName.substring(0, endIndex);
        }
        String clazzName = null;
        if (clazz != null) {
            clazzName = pkgName + "." + clazz;
        }
        return clazzName;
    }

    private static void addClassName(List<Class<?>> clazzList, String clazzName, Class<? extends Annotation> annotation,
            ClassLoader cl) {
        if (clazzList != null && clazzName != null) {
            Class<?> clazz = null;
            try {
                if (clazzName.endsWith(".class")) {
                    clazzName = clazzName.substring(0, clazzName.lastIndexOf("."));
                }
                clazz = Class.forName(clazzName, true, cl);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            if (clazz != null) {
                if (annotation == null) {
                    clazzList.add(clazz);
                } else if (clazz.isAnnotationPresent(annotation)) {
                    clazzList.add(clazz);
                }
            }
        }
    }

    public static Properties getProperties(String configFile) {
        Properties configProperties = new Properties();
        try (InputStream ins = new FileInputStream(configFile)) {
            configProperties.load(ins);
        } catch (IOException e) {
            logger.error("read config {} properties error", configFile, e);
        }
        return configProperties;
    }
}
