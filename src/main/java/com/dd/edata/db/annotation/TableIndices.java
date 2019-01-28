package com.dd.edata.db.annotation;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * 创建多个索引
 * 
 */
@Target(value = { java.lang.annotation.ElementType.TYPE })
@Retention(value = java.lang.annotation.RetentionPolicy.RUNTIME)
@Inherited
public @interface TableIndices {

    /**
     * 索引数组，可以定义多个索引
     * 
     * @return
     */
    public TableIndex[] value();
}