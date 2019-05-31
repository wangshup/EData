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

    public static DBWhere EQ(String name, Object value) {
        return new DBWhere(name, value, WhereCond.EQ);
    }

    public static DBWhere NEQ(String name, Object value) {
        return new DBWhere(name, value, WhereCond.NEQ);
    }

    public static DBWhere IN(String name, List<?> values) {
        return new DBWhere(name, values, WhereCond.IN);
    }

    public static DBWhere LT(String name, Object value) {
        return new DBWhere(name, value, WhereCond.LT);
    }

    public static DBWhere LE(String name, Object value) {
        return new DBWhere(name, value, WhereCond.LE);
    }

    public static DBWhere GT(String name, Object value) {
        return new DBWhere(name, value, WhereCond.GT);
    }

    public static DBWhere GE(String name, Object value) {
        return new DBWhere(name, value, WhereCond.GE);
    }

    public static DBWhere LIKE(String name, Object value) {
        return new DBWhere(name, value, WhereCond.LIKE);
    }

    public static DBWhere ORDER_DESC(String name) {
        return new DBWhere(name, null, WhereCond.ORDER_DESC);
    }

    public static DBWhere ORDER_ASC(String name) {
        return new DBWhere(name, null, WhereCond.ORDER_ASC);
    }

    public static DBWhere LIMIT(Object value) {
        return new DBWhere(null, value, WhereCond.LIMIT);
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
        EQ, NEQ, IN, LT, LE, GT, GE, LIKE, ORDER_ASC, ORDER_DESC, LIMIT
    }
}
