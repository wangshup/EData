package com.dd.edata.db.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 字段类型声明
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

    /**
     * 数据库表字段名字
     * 
     * @return
     */
    public String name() default "";

    /**
     * 该字段的数据类型
     * 
     * @return
     */
    public String type() default "";

    /**
     * 该字段的长度
     * 
     * @return
     */
    public int len() default 0;

    /**
     * 浮点数据的精度值，默认为6位
     * 
     * @return
     */
    public int precision() default 6;

    /**
     * 是否可以为空
     * 
     * @return
     */
    public boolean isNull() default true;

    /**
     * 默认值
     * 
     * @return
     */
    public String defaultValue() default "";

    /**
     * 是否有默认值
     * 
     * @return
     */
    public boolean hasDefault() default false;

    /**
     * 字段含义描述
     * 
     * @return
     */
    public String comment() default "";

    /**
     * 是否转化成json格式
     * 
     * @return
     */
    public boolean isJson() default false;

    /**
     * 是否大小写敏感
     * 
     * @return
     */
    public boolean charSens() default false;

    /**
     * 是否是自增
     * 
     * @return
     */
    public boolean autoIncrement() default false;
};
