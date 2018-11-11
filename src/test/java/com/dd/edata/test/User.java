package com.dd.edata.test;

import com.dd.edata.db.annotation.Column;
import com.dd.edata.db.annotation.Table;
import com.dd.edata.db.annotation.TablePrimaryKey;

import java.util.Calendar;
import java.util.Date;

@Table(name = "user")
@TablePrimaryKey(members = {"id"})
public class User {
    @Column(isNull = false)
    private long id;

    @Column(type = "varchar", len = 12)
    private String name;

    @Column(name = "create_time")
    private Date createTime;

    public User() {
    }

    public User(long id, String name) {
        this.id = id;
        this.name = name;
        this.createTime = Calendar.getInstance().getTime();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "User{" + "id=" + id + ", name='" + name + '\'' + ", createTime=" + createTime + '}';
    }
}
