# 数据服务层（适合游戏、应用等需要数据库服务的地方）


### 特性

* 使用注解配置，数据库表的创建和修改完全自动化
* 提供了诸多数据操作接口，支持同步和异步数据操作
* 异步操作提供日志支持，避免服务器宕机后操作的丢失，服务器启动后自动恢复
* 完全无SQL化操作，减少服务器的数据库和SQL维护工作

### 初始化和启动非常简单

```java
//初始化并启动EData
EData edata = EData.start(0, "com.dd.edata.test", EDataTest.class.getClassLoader(), getConfigProperties("config.properties"));
```

### 程序代码示例

## Bean类

```java
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

    
    @Override
    public String toString() {
        return "User{" + "id=" + id + ", name='" + name + '\'' + ", createTime=" + createTime + '}';
    }
}
```

## Main类

```java
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
```