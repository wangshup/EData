package com.dd.edata.db.annotation;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 表声明
 * 
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {

    /** 正常策略，表名字自定义，表名字=name()，默认策略为该策略 */
    public static final int POLICY_NORMAL = 0;
    /** 按服务器ID建表 */
    public static final int POLICY_SERVER_ID = 1;
    /** 按月建表策略；表名字格式：name()_year_month ;month 取值[1,12] */
    public static final int POLICY_YEAR_MONTH = 2;
    /** 按天建表策略；表名字格式：name()_year_month_day; month 取值[1,12]，day 取值[1,31] */
    public static final int POLICY_YEAR_MONTH_DAY = 3;

    /**
     * DB中的表名字
     * 
     * @return
     */
    public String name() default "";

    /**
     * <pre>
     * 建表策略，一般情况下采用默认策略；
     * 策略参考上面的常量定义
     * </pre>
     * 
     * @return
     */
    public int policy() default POLICY_NORMAL;

    /**
     * 按月或按天建表策略时，一次性建立表的数量
     * @return
     */
    public int count() default 7;
};
