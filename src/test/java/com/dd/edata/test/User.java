package com.dd.edata.test;

import com.dd.edata.db.annotation.*;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

@Table(name = "user")
@TablePrimaryKey(members = {"id"})
@TableIndices(@TableIndex(name = "idx_test", members = {"height", "weight", "money"}))
public class User {
    @Column(isNull = false, autoIncrement = false)
    private long id;

    @Column(type = "varchar", len = 12)
    private String name;

    @Column(name = "create_time")
    private Date createTime;

    @Column
    private int height;

    @Column
    private int money;

    @Column
    private int weight;

    public User() {
    }

    public User(long id, String name) {
        this.id = id;
        this.name = name;
        this.createTime = Calendar.getInstance().getTime();
        this.height = ThreadLocalRandom.current().nextInt(100, 200);
        this.weight = ThreadLocalRandom.current().nextInt(30, 200);
        this.money = ThreadLocalRandom.current().nextInt(0, 10000000);
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
        return "User{" + "id=" + id + ", name='" + name + '\'' + ", createTime=" + createTime + ", height=" + height + ", money=" + money + ", weight=" + weight + '}';
    }
}
