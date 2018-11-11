package com.dd.edata.db;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public final class DBDataType {

    public static final Map<Class<?>, String> TYPE_MAP = new HashMap<Class<?>, String>() {
        private static final long serialVersionUID = 1L;

        {
            put(Byte.class, "tinyint");
            put(byte.class, "tinyint");
            put(Short.class, "smallint");
            put(short.class, "smallint");
            put(Integer.class, "int");
            put(int.class, "int");
            put(Long.class, "bigint");
            put(long.class, "bigint");
            put(Float.class, "float");
            put(float.class, "float");
            put(Double.class, "double");
            put(double.class, "double");
            put(Boolean.class, "tinyint");
            put(boolean.class, "tinyint");
            put(String.class, "text");
            put(Date.class, "timestamp");
            put(byte[].class, "blob");
        }
    };
}
