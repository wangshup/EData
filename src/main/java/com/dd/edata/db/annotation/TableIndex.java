package com.dd.edata.db.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 索引的标识，此标识声明在DB实体类上
 * 
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface TableIndex {

    /**
     * 索引名字，不要超过64个字符
     */
    public String name();

    /**
     * 索引数据类型，只支持简单数据类型作为索引
     * 
     * @return
     */
    public java.lang.String[] members();

}
