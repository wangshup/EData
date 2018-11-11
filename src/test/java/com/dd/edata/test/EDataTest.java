package com.dd.edata.test;

import com.dd.edata.EData;
import com.dd.edata.db.DBWhere;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EDataTest {

    public static void main(String[] args) throws Exception {
        //初始化并启动EData
        EData edata = EData.start(0, "com.dd.edata.test", EDataTest.class.getClassLoader(), getConfigProperties("config.properties"));

        //同步插入一条数据
        edata.insert(new User(0, "name0"));

        //异步插入一条数据
        edata.insertAsync(new User(1, "name1"));

        //同步批量插入一组数据
        int i = 2;
        List<User> list = new ArrayList<>();
        for (; i < 10; i++) {
            list.add(new User(i, "name" + i));
        }
        edata.insertBatch(list);

        //异步批量插入一组数据
        list.clear();
        for (; i < 20; i++) {
            list.add(new User(i, "name" + i));
        }
        edata.insertBatchAsync((result) -> {
            System.out.println(((int[]) result).length + " datas async inserted successfully");
        }, list);

        //同步查询一条数据
        User u = edata.select(User.class, DBWhere.equal("id", 0));
        System.out.println(u);

        //异步查询一条数据
        edata.selectAsync((user) -> {
            System.out.println(user);
        }, User.class, DBWhere.equal("id", 1));

        //异步查询一组数据
        edata.selectListAsync((users) -> {
            System.out.println(users);
            edata.shutdown();
        }, User.class, new DBWhere("id", 10, DBWhere.WhereCond.GE));
    }

    public static Properties getConfigProperties(String fileName) {
        Properties configProperties;
        configProperties = new Properties();
        try {
            configProperties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            System.out.println(e.getCause());
        }

        return configProperties;
    }
}
