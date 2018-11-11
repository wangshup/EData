package com.dd.edata.db;

import java.util.Comparator;
import java.util.List;

/**
 * 数据库查询辅助类
 *
 * @author wangshupeng
 */
public class DBWhere {
    public static final Comparator<DBWhere> comparator = new Comparator<DBWhere>() {
        @Override
        public int compare(DBWhere o1, DBWhere o2) {
            return o1.cond.compareTo(o2.cond);
        }
    };
    private String name;
    private Object value;
    private WhereCond cond;

    DBWhere() {
    }

    public DBWhere(String name, Object value, WhereCond cond) {
        this.name = name;
        this.value = value;
        this.cond = cond;
    }

    public static DBWhere equal(String name, Object value) {
        return new DBWhere(name, value, WhereCond.EQ);
    }

    public static DBWhere limit(Object value) {
        return new DBWhere(null, value, WhereCond.LIMIT);
    }

    public static DBWhere orderDesc(String name) {
        return new DBWhere(name, null, WhereCond.ORDER_DESC);
    }

    public static DBWhere orderAsc(String name) {
        return new DBWhere(name, null, WhereCond.ORDER_ASC);
    }

    public static DBWhere in(String name, List<?> values) {
        return new DBWhere(name, values, WhereCond.IN);
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public WhereCond getCond() {
        return cond;
    }

    public enum WhereCond {
        EQ, IN, LT, LE, GT, GE, LIKE, ORDER_ASC, ORDER_DESC, LIMIT
    }
}
