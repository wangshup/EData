package com.dd.edata.db;

import com.dd.edata.db.DBWhere.WhereCond;
import com.dd.edata.db.annotation.*;
import com.dd.edata.utils.Util;
import com.zaxxer.hikari.HikariConfigMXBean;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

/**
 * 数据库服务类
 *
 * @author wangshupeng
 */
final class DBService {
    static final byte DB_NOOP = 0;
    static final byte DB_INSERT = 1;
    static final byte DB_INSERT_BATCH = 2;
    static final byte DB_UPDATE = 3;
    static final byte DB_UPDATE_BATCH = 4;
    static final byte DB_UPDATE_WHERE = 5;
    static final byte DB_DELETE = 6;
    static final byte DB_DELETE_BATCH = 7;
    static final byte DB_DELETE_WHERE = 8;
    static final byte DB_TRUNCATE = 9;
    static final byte DB_TX_COMMIT = 10;

    private static final Logger logger = LoggerFactory.getLogger(DBService.class);
    private static final ThreadLocal<Calendar> calHolder = ThreadLocal.withInitial(() -> Calendar.getInstance());
    private DataSource dataSource;
    private DBUtil dbUtil;
    private boolean isCobar;

    DBService(IDBProxy proxy, String packagePath, ClassLoader cl, DataSource ds, boolean isCobar) {
        dataSource = ds;
        this.isCobar = isCobar;
        dbUtil = new DBUtil();
        dbUtil.init(proxy, packagePath, cl);
    }

    protected void propertiesReload(Properties props) {
        HikariConfigMXBean bean = ((HikariDataSource) this.dataSource).getHikariConfigMXBean();
        bean.setConnectionTimeout(Long.parseLong(props.getProperty("db.connectionTimeout", "1000")));
        bean.setMaximumPoolSize(Integer.parseInt(props.getProperty("db.maxPoolSize", "32")));
        bean.setMinimumIdle(Integer.parseInt(props.getProperty("db.minIdle", "4")));
        bean.setLeakDetectionThreshold(Long.parseLong(props.getProperty("db.leakDetectionThreshold", "30000")));
    }

    /**
     * 查询一条数据
     *
     * @param clazz
     * @return
     * @throws Exception
     */
    protected <T> T select(Class<T> clazz, List<String> columns, DBWhere... wheres) throws Exception {
        DBWhere[] newWheres = new DBWhere[wheres.length + 1];
        System.arraycopy(wheres, 0, newWheres, 0, wheres.length);
        newWheres[wheres.length] = DBWhere.LIMIT(1);
        List<T> l = selectList(clazz, columns, newWheres);
        if (l == null || l.isEmpty()) {
            return null;
        }
        return l.get(0);
    }

    /**
     * 查询返回list
     *
     * @param clazz
     * @return
     */
    protected <T> List<T> selectList(Class<T> clazz, List<String> columns, DBWhere... wheres) throws Exception {
        Arrays.sort(wheres, DBWhere.comparator);
        List<T> retList = new ArrayList<>();
        String sql = makeSelectSql(clazz, columns, wheres);
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            try {
                if (wheres != null && wheres.length > 0) {
                    int index = 1;
                    for (DBWhere where : wheres) {
                        if (where.getCond() == WhereCond.LIMIT || where.getCond() == WhereCond.ORDER_ASC || where.getCond() == WhereCond.ORDER_DESC) {
                            break;
                        }
                        if (where.getCond() == WhereCond.IN) {
                            List<?> values = (List<?>) where.getValue();
                            for (int i = 0; i < values.size(); ++i) {
                                stmt.setObject(index++, values.get(i));
                            }
                        } else {
                            stmt.setObject(index++, where.getValue());
                        }
                    }
                }

                ResultSet rs = stmt.executeQuery();
                if (rs != null) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    while (rs.next()) {
                        T bean = clazz.getDeclaredConstructor().newInstance();
                        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                            String columnName = rsmd.getColumnName(i);
                            Object columnValue = rs.getObject(i);
                            int type = rsmd.getColumnType(i);
                            if (type == 93 && columnValue != null) {
                                Calendar cl = Calendar.getInstance();
                                cl.setTimeInMillis(rs.getTimestamp(i).getTime());
                                columnValue = cl.getTime();
                            }
                            Map<String, Field> fields = dbUtil.getFields(clazz);
                            for (Field field : fields.values()) {
                                if (columnName.equalsIgnoreCase(Util.getColName(field)) && columnValue != null) {
                                    Column ann = field.getAnnotation(Column.class);
                                    BeanUtils.setProperty(bean, field.getName(), ann.isJson() ? Util.fromJson(columnValue.toString(), field.getGenericType()) : columnValue);
                                    break;
                                }
                            }
                        }
                        retList.add(bean);
                    }
                }
            } catch (Exception e) {
                throw e;
            }
        }
        return retList;
    }

    private String makeSelectSql(Class<?> clazz, List<String> columns, DBWhere... wheres) throws Exception {
        String tableName = dbUtil.getTableName(clazz);
        StringBuilder sb = new StringBuilder();
        if (columns == null || columns.isEmpty()) {
            sb.append("SELECT * FROM ");
        } else {
            Map<String, Field> fields = dbUtil.getFields(clazz);
            sb.append("SELECT ");
            String delimiter = "";
            for (String column : columns) {
                sb.append(delimiter);
                Field f = fields.getOrDefault(column, null);
                if (f != null) {
                    sb.append(Util.getColName(f));
                } else {
                    sb.append(column);
                }
                delimiter = ",";
            }
            sb.append(" FROM ");
        }
        sb.append(tableName).append(makeWhere(clazz, wheres));
        return sb.toString();
    }

    /**
     * 查询返回数量
     *
     * @return
     */
    protected <T> int count(Class<T> clazz, DBWhere... wheres) throws Exception {
        Arrays.sort(wheres, DBWhere.comparator);
        String sql = makeCountSql(clazz, wheres);
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql);) {
            try {
                if (wheres != null && wheres.length > 0) {
                    int index = 1;
                    for (DBWhere where : wheres) {
                        if (where.getCond() == WhereCond.LIMIT || where.getCond() == WhereCond.ORDER_ASC || where.getCond() == WhereCond.ORDER_DESC) {
                            break;
                        }
                        if (where.getCond() == WhereCond.IN) {
                            List<?> values = (List<?>) where.getValue();
                            for (int i = 0; i < values.size(); ++i) {
                                stmt.setObject(index++, values.get(i));
                            }
                        } else {
                            stmt.setObject(index++, where.getValue());
                        }
                    }
                }

                ResultSet rs = stmt.executeQuery();
                if (rs != null) {
                    while (rs.next()) return rs.getInt(1);
                }
            } catch (Exception e) {
                throw e;
            }
        }
        return 0;
    }

    private String makeCountSql(Class<?> clazz, DBWhere... wheres) throws Exception {
        String tableName = dbUtil.getTableName(clazz);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) FROM ").append(tableName);
        sb.append(makeWhere(clazz, wheres));
        return sb.toString();
    }

    private String makeWhere(Class<?> clazz, DBWhere... wheres) {
        StringBuilder sb = new StringBuilder();
        if (wheres != null && wheres.length > 0) {
            sb.append(" WHERE 1 = 1 ");
            label:
            for (DBWhere where : wheres) {
                Field f = dbUtil.getFields(clazz).getOrDefault(where.getName(), null);
                String columnName = f == null ? where.getName() : Util.getColName(f);
                switch (where.getCond()) {
                    case EQ:
                        sb.append(" AND ").append(columnName).append(" = ?");
                        break;
                    case LT:
                        sb.append(" AND ").append(columnName).append(" < ?");
                        break;
                    case LE:
                        sb.append(" AND ").append(columnName).append(" <= ?");
                        break;
                    case GT:
                        sb.append(" AND ").append(columnName).append(" > ?");
                        break;
                    case GE:
                        sb.append(" AND ").append(columnName).append(" >= ?");
                        break;
                    case LIKE:
                        sb.append(" AND ").append(columnName).append(" like ?");
                        break;
                    case ORDER_ASC:
                        sb.append(" order by ").append(columnName).append(" asc");
                        break;
                    case ORDER_DESC:
                        sb.append(" order by ").append(columnName).append(" desc");
                        break;
                    case LIMIT:
                        sb.append(" LIMIT ").append(where.getValue());
                        break label;
                    case IN:
                        sb.append(" AND ").append(columnName).append(" IN ( ");
                        List<?> values = (List<?>) where.getValue();
                        String delimiter = "";
                        for (int i = 0; i < values.size(); ++i) {
                            sb.append(delimiter);
                            sb.append("?");
                            delimiter = ", ";
                        }
                        sb.append(" ) ");
                        break;
                    default:
                        break;
                }
            }
        }
        // sb.append(";");
        return sb.toString();
    }

    /**
     * 同步删除数据
     */
    protected <T> boolean delete(Class<T> clazz, DBWhere... wheres) throws Exception {
        Arrays.sort(wheres, DBWhere.comparator);
        String sql = makeDeleteSql(clazz, wheres);
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql);) {
            try {
                if (wheres != null && wheres.length > 0) {
                    int index = 1;
                    for (DBWhere where : wheres) {
                        if (where.getCond() == WhereCond.LIMIT || where.getCond() == WhereCond.ORDER_ASC || where.getCond() == WhereCond.ORDER_DESC) {
                            break;
                        }
                        if (where.getCond() == WhereCond.IN) {
                            List<?> values = (List<?>) where.getValue();
                            for (int i = 0; i < values.size(); ++i) {
                                stmt.setObject(index++, values.get(i));
                            }
                        } else {
                            stmt.setObject(index++, where.getValue());
                        }
                    }
                }
                return stmt.executeUpdate() > 0;
            } catch (Exception e) {
                throw e;
            }
        }
    }

    /**
     * 同步删除一条数据
     */
    protected <T> boolean delete(T t) throws Exception {
        String sql = makeDeleteSql(t.getClass());
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql);) {
            try {
                Iterator<Field> primaryKeyIter = dbUtil.getPrimaryKeys(t.getClass()).iterator();
                int index = 1;
                while (primaryKeyIter.hasNext()) {
                    Field f = primaryKeyIter.next();
                    Column ann = f.getAnnotation(Column.class);
                    stmt.setObject(index++, ann.isJson() ? Util.toJson(f.get(t)) : f.get(t));
                }
                return stmt.executeUpdate() > 0;
            } catch (Exception e) {
                throw e;
            }
        }
    }

    /**
     * 批量删除操作
     */
    protected <T> int[] batchDelete(List<T> objs) throws Exception {
        if (objs.isEmpty()) {
            return new int[]{};
        }
        String sql = makeDeleteSql(objs.get(0).getClass());
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql);) {
            try {
                conn.setAutoCommit(false);
                for (T o : objs) {
                    Iterator<Field> primaryKeyIter = dbUtil.getPrimaryKeys(o.getClass()).iterator();
                    int index = 1;
                    while (primaryKeyIter.hasNext()) {
                        Field f = primaryKeyIter.next();
                        Column ann = f.getAnnotation(Column.class);
                        stmt.setObject(index++, ann.isJson() ? Util.toJson(f.get(o)) : f.get(o));
                    }
                    stmt.addBatch();
                }
                int[] ret = stmt.executeBatch();
                conn.commit();
                return ret;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private <T> String makeDeleteSql(Class<T> clazz, DBWhere... wheres) throws Exception {
        String tableName = dbUtil.getTableName(clazz);
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(tableName);
        sb.append(makeWhere(clazz, wheres));
        return sb.toString();
    }

    private <T> String makeDeleteSql(Class<T> clazz) throws Exception {
        String tableName = dbUtil.getTableName(clazz);
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(tableName);
        sb.append(makePrimaryWhereCondition(clazz));
        return sb.toString();
    }

    /**
     * 根据 primaryKey生成 where条件
     *
     * @return
     */
    private <T> String makePrimaryWhereCondition(Class<T> clazz) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(" WHERE 1 = 1 ");
        String tableName = dbUtil.getTableName(clazz);
        boolean hadCondition = false;
        Iterator<Field> primaryKeyIter = dbUtil.getPrimaryKeys(clazz).iterator();
        while (primaryKeyIter.hasNext()) {
            Field f = primaryKeyIter.next();
            sb.append(" AND ").append(Util.getColName(f)).append(" = ?");
            hadCondition = true;
        }
        if (!hadCondition) {
            logger.error("[DBService]: error, a sql no any condition @table " + tableName);
            throw new IllegalArgumentException("error:a sql no any condition @table " + tableName);
        }
        return sb.toString();
    }

    /**
     * 同步更新数据
     *
     * @param clazz
     * @param name
     * @param value
     * @param wheres
     * @param <T>
     * @return
     * @throws Exception
     */
    protected <T> int update(Class<T> clazz, String name, Object value, DBWhere... wheres) throws Exception {
        Arrays.sort(wheres, DBWhere.comparator);
        String sql = makeUpdateSql(clazz, name, wheres);
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql);) {
            try {
                int index = 1;
                stmt.setObject(index++, value);
                if (wheres != null && wheres.length > 0) {
                    for (DBWhere where : wheres) {
                        if (where.getCond() == WhereCond.LIMIT || where.getCond() == WhereCond.ORDER_ASC || where.getCond() == WhereCond.ORDER_DESC) {
                            break;
                        }
                        if (where.getCond() == WhereCond.IN) {
                            List<?> values = (List<?>) where.getValue();
                            for (int i = 0; i < values.size(); ++i) {
                                stmt.setObject(index++, values.get(i));
                            }
                        } else {
                            stmt.setObject(index++, where.getValue());
                        }
                    }
                }
                return stmt.executeUpdate();
            } catch (Exception e) {
                throw e;
            }
        }
    }

    /**
     * 同步更新数据
     *
     * @param t
     * @throws Exception
     */
    protected <T> int update(T t) throws Exception {
        String sql = makeUpdateSql(t.getClass());
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql);) {
            try {
                Map<String, Field> fieldMap = dbUtil.getFields(t.getClass());
                int index = 1;
                for (Field f : fieldMap.values()) {
                    Column ann = f.getAnnotation(Column.class);
                    stmt.setObject(index++, ann.isJson() ? Util.toJson(f.get(t)) : f.get(t));
                }

                Iterator<Field> primaryKeyIter = dbUtil.getPrimaryKeys(t.getClass()).iterator();
                while (primaryKeyIter.hasNext()) {
                    Field f = primaryKeyIter.next();
                    Column ann = f.getAnnotation(Column.class);
                    stmt.setObject(index++, ann.isJson() ? Util.toJson(f.get(t)) : f.get(t));
                }
                return stmt.executeUpdate();
            } catch (Exception e) {
                throw e;
            }
        }
    }

    /**
     * 批量更新
     */
    protected <T> int[] batchUpdate(List<T> objs) throws Exception {
        if (objs.isEmpty()) {
            return new int[]{};
        }

        String sql = makeUpdateSql(objs.get(0).getClass());
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql);) {
            try {
                conn.setAutoCommit(false);
                for (T o : objs) {
                    Map<String, Field> fieldMap = dbUtil.getFields(o.getClass());
                    int index = 1;
                    for (Field f : fieldMap.values()) {
                        Column ann = f.getAnnotation(Column.class);
                        stmt.setObject(index++, ann.isJson() ? Util.toJson(f.get(o)) : f.get(o));
                    }

                    Iterator<Field> primaryKeyIter = dbUtil.getPrimaryKeys(o.getClass()).iterator();
                    while (primaryKeyIter.hasNext()) {
                        Field f = primaryKeyIter.next();
                        Column ann = f.getAnnotation(Column.class);
                        stmt.setObject(index++, ann.isJson() ? Util.toJson(f.get(o)) : f.get(o));
                    }
                    stmt.addBatch();
                }
                int[] ret = stmt.executeBatch();
                conn.commit();
                return ret;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private <T> String makeUpdateSql(Class<T> clazz, String name, DBWhere... wheres) throws Exception {
        String tableName = dbUtil.getTableName(clazz);
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(tableName).append(" SET ");
        Field f = dbUtil.getFields(clazz).get(name);
        sb.append(Util.getColName(f)).append(" = ? ");
        sb.append(makeWhere(clazz, wheres));
        return sb.toString();
    }

    private <T> String makeUpdateSql(Class<T> clazz) throws Exception {
        String tableName = dbUtil.getTableName(clazz);
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(tableName).append(" SET ");
        Map<String, Field> fieldMap = dbUtil.getFields(clazz);
        String separator = "";
        for (Field f : fieldMap.values()) {
            sb.append(separator);
            sb.append(Util.getColName(f)).append(" = ?");
            separator = ",";
        }

        sb.append(makePrimaryWhereCondition(clazz));
        return sb.toString();
    }

    /**
     * 同步插入
     *
     * @throws Exception
     */
    protected <T> boolean insertOrReplace(T t, boolean replace) throws Exception {
        String sql = makeInsertOrReplaceSql(t.getClass(), replace);
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
            try {
                Map<String, Field> fieldMap = dbUtil.getFields(t.getClass());
                int index = 1;
                Field autoIncrField = null;
                for (Field f : fieldMap.values()) {
                    Column ann = f.getAnnotation(Column.class);
                    if (ann.autoIncrement()) {
                        autoIncrField = f;
                        continue;
                    }
                    stmt.setObject(index++, ann.isJson() ? Util.toJson(f.get(t)) : f.get(t));
                }
                boolean result = stmt.executeUpdate() > 0;
                if (autoIncrField != null) {
                    ResultSet rs = stmt.getGeneratedKeys();
                    if (rs.next()) {
                        BeanUtils.copyProperty(t, autoIncrField.getName(), rs.getObject(1));
                    }
                }
                return result;
            } catch (Exception e) {
                throw e;
            }
        }
    }

    /**
     * 批量插入操作
     */
    protected <T> int[] batchInsertOrReplace(List<T> objs, boolean replace) throws Exception {
        if (objs.isEmpty()) {
            return new int[]{};
        }

        String sql = makeInsertOrReplaceSql(objs.get(0).getClass(), replace);
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
            try {
                conn.setAutoCommit(false);
                Field autoIncrField = null;
                for (T o : objs) {
                    Map<String, Field> fieldMap = dbUtil.getFields(o.getClass());
                    int index = 1;
                    for (Field f : fieldMap.values()) {
                        Column ann = f.getAnnotation(Column.class);
                        if (ann.autoIncrement()) {
                            autoIncrField = f;
                            continue;
                        }
                        stmt.setObject(index++, ann.isJson() ? Util.toJson(f.get(o)) : f.get(o));
                    }
                    stmt.addBatch();
                }

                int[] ret = stmt.executeBatch();
                conn.commit();
                if (autoIncrField != null) {
                    ResultSet rs = stmt.getGeneratedKeys();
                    for (T o : objs) {
                        if (!rs.next()) {
                            break;
                        }
                        BeanUtils.copyProperty(o, autoIncrField.getName(), rs.getObject(1));
                    }
                }
                return ret;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private <T> String makeInsertOrReplaceSql(Class<T> clazz, boolean replace) throws Exception {
        String tableName = dbUtil.getTableName(clazz);
        StringBuilder sb = new StringBuilder();
        StringBuilder sbValues = new StringBuilder();
        if (replace)
            sb.append("REPLACE INTO ").append(tableName).append(" (");
        else
            sb.append("INSERT INTO ").append(tableName).append(" (");
        sbValues.append(") VALUES (");
        Map<String, Field> fieldMap = dbUtil.getFields(clazz);
        String separator = "";
        for (Field f : fieldMap.values()) {
            Column colann = f.getAnnotation(Column.class);
            if (colann != null && colann.autoIncrement()) {
                continue;
            }
            sb.append(separator);
            sb.append(Util.getColName(f));
            sbValues.append(separator);
            sbValues.append("?");
            separator = ",";
        }

        sb.append(sbValues).append(")");
        return sb.toString();
    }


    protected <T> boolean truncate(Class<T> clazz) throws Exception {
        String tableName = dbUtil.getTableName(clazz);
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement((isCobar ? "delete from " : "TRUNCATE TABLE ") + tableName);) {
            try {
                 stmt.executeUpdate();
                 return true;
            } catch (Exception e) {
                throw e;
            }
        }
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private class DBUtil {
        private Map<String, String> tableNames = new HashMap<>();
        private Map<Class<?>, Map<String, Field>> fields = new HashMap<>();
        private Map<Class<?>, Set<Field>> primaryKeys = new HashMap<>();
        private IDBProxy dbProxy;

        private String getTableName(Class<?> clazz) {
            Table tna = clazz.getAnnotation(Table.class);
            switch (tna.policy()) {
                case Table.POLICY_YEAR_MONTH:
                case Table.POLICY_YEAR_MONTH_DAY: {
                    Calendar cal = calHolder.get();
                    cal.setTimeInMillis(System.currentTimeMillis());
                    return getTableName(clazz, cal);
                }
                default:
                    break;
            }
            return tableNames.get(clazz.getName());
        }

        private Set<Field> getPrimaryKeys(Class<?> clazz) {
            return primaryKeys.get(clazz);
        }

        private Map<String, Field> getFields(Class<?> clazz) {
            if (fields.containsKey(clazz)) {
                return fields.get(clazz);
            }
            Map<String, Field> fieldMap = new LinkedHashMap<>();
            Set<Field> pks = new LinkedHashSet<>();
            List<Field> fieldList = new ArrayList<>();
            getField(clazz, fieldList);
            TablePrimaryKey primaryKey = (TablePrimaryKey) clazz.getAnnotation(TablePrimaryKey.class);
            List<String> primaryKeyList = null;
            if (primaryKey != null) {
                primaryKeyList = new ArrayList<String>();
                for (String pk : primaryKey.members()) {
                    primaryKeyList.add(pk);
                }
            }
            for (Field f : fieldList) {
                Column colann = f.getAnnotation(Column.class);
                if (colann == null) {
                    continue;
                }
                String colName = Util.getColName(f);
                f.setAccessible(true);
                fieldMap.put(f.getName(), f);
                if (primaryKeyList != null) {
                    for (String pk : primaryKeyList) {
                        if (colName.equalsIgnoreCase(pk)) {
                            pks.add(f);
                        }
                    }
                }
            }
            primaryKeys.put(clazz, pks);
            fields.put(clazz, fieldMap);
            return fieldMap;
        }

        private void init(IDBProxy proxy, String packagePath, ClassLoader cl) {
            this.dbProxy = proxy;
            initTables(Util.getClassList(packagePath, true, null, cl));
        }

        private void initTables(List<Class<?>> dbTables) {
            try {
                for (Class<?> c : dbTables) {
                    initTables(c);
                }
            } catch (Exception e) {
                logger.error("init table error!", e);
            }
        }

        private boolean initTables(Class<?> clazz) throws Exception {
            if (clazz.getAnnotation(Table.class) == null) {
                return false;
            }
            // 创建基本表
            Calendar cal = calHolder.get();
            String tableName = getTableName(clazz, cal);
            tableNames.put(clazz.getName(), tableName);
            createTable(clazz, tableName);
            checkTable(clazz, tableName);

            Table tna = clazz.getAnnotation(Table.class);
            switch (tna.policy()) {
                case Table.POLICY_YEAR_MONTH: {
                    for (int i = 0; i < tna.count(); ++i) {
                        cal.add(Calendar.MONTH, 1);
                        tableName = getTableName(clazz, cal);
                        createTable(clazz, tableName);
                        checkTable(clazz, tableName);
                    }
                    break;
                }
                case Table.POLICY_YEAR_MONTH_DAY: {
                    for (int i = 0; i < tna.count(); i++) {
                        cal.add(Calendar.DAY_OF_MONTH, 1);
                        tableName = getTableName(clazz, cal);
                        createTable(clazz, tableName);
                        checkTable(clazz, tableName);
                    }
                    break;
                }
                default:
                    break;
            }
            return true;
        }

        private String getTableName(Class<?> clazz, Calendar cal) {
            Table tna = clazz.getAnnotation(Table.class);
            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH) + 1;

            String tableName = (tna.name() != null && !tna.name().equals("")) ? tna.name() : clazz.getSimpleName();
            if (tna.policy() == Table.POLICY_SERVER_ID) {
                tableName = tna.name() + "_" + dbProxy.getSid();
            } else if (tna.policy() == Table.POLICY_YEAR_MONTH) {
                tableName = tna.name() + "_" + year + "_" + month;
            } else if (tna.policy() == Table.POLICY_YEAR_MONTH_DAY) {
                int day = cal.get(Calendar.DAY_OF_MONTH);
                tableName = tna.name() + "_" + year + "_" + month + "_" + day;
            }

            return tableName;
        }

        /**
         * 1 判断是否有新追加的字段 alter
         *
         * @param clazz
         * @param tableName
         */
        private void checkTable(Class<?> clazz, String tableName) throws Exception {
            StringBuffer sb = new StringBuffer();
            sb.append("select COLUMN_NAME, COLUMN_TYPE from information_schema.columns where table_name = '");
            sb.append(tableName);
            if (!isCobar) {
                sb.append("' and table_schema = '");
            }
            try (Connection conn = getConnection();) {
                if (!isCobar) {
                    sb.append(conn.getCatalog());
                }
                sb.append("'");
                try (PreparedStatement stmt = conn.prepareStatement(sb.toString())) {
                    ResultSet rs = stmt.executeQuery();
                    Map<String, List<String>> columns = new HashMap<>();
                    while (rs.next()) {
                        String k = rs.getString(1).toUpperCase();
                        String v = rs.getString(2).toLowerCase();
                        if (columns.containsKey(k)) {
                            columns.get(k).add(v);
                        } else {
                            List<String> l = new ArrayList<>();
                            l.add(v);
                            columns.put(k, l);
                        }
                    }
                    Map<String, Field> fields = getFields(clazz);
                    for (Field f : fields.values()) {
                        Column ann = f.getAnnotation(Column.class);
                        if (ann == null) {
                            continue;
                        }
                        String colName = Util.getColName(f).toUpperCase();
                        if (!columns.containsKey(colName)) {
                            addField(f, tableName);
                        } else {
                            String type = Util.getColTypeWithLength(f);
                            for (String colType : columns.get(colName)) {
                                if (!colType.startsWith(type)) {
                                    modifyField(f, tableName);
                                    break;
                                }
                            }

                        }
                    }
                }
            }
        }

        private void addField(Field f, String tableName) throws SQLException {
            String addSql = "alter table " + tableName + " add " + makeFieldSql(f);
            logger.info("[DBService]: alter table:{}", addSql);
            try (Connection conn = getConnection();
                 PreparedStatement stmt = conn.prepareStatement(addSql)) {
                stmt.execute();
            }
        }

        private void modifyField(Field f, String tableName) throws SQLException {
            String addSql = "alter table " + tableName + " MODIFY COLUMN " + makeFieldSql(f);
            logger.info("[DBService]: alter table:{}", addSql);
            try (Connection conn = getConnection();
                 PreparedStatement stmt = conn.prepareStatement(addSql)) {
                stmt.execute();
            }
        }

        /**
         * 反射机制创建一个表
         *
         * @throws Exception
         */
        private void createTable(Class<?> c, String tableName) throws Exception {
            TableIndices tableIndices = c.getAnnotation(TableIndices.class);
            TableIndex[] tableIndexList = null;
            if (tableIndices != null) {
                tableIndexList = tableIndices.value();
            }
            Map<String, Field> fields = getFields(c);
            TablePrimaryKey primaryKey = c.getAnnotation(TablePrimaryKey.class);
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName);
            sql.append("(");
            String sperator = "";
            for (Field f : fields.values()) {
                sql.append(sperator);
                sql.append(makeFieldSql(f));
                sperator = ", ";
            }
            if (primaryKey != null) {
                StringBuilder primaryKeyMemebers = new StringBuilder();
                sperator = "";
                for (String member : primaryKey.members()) {
                    primaryKeyMemebers.append(sperator);
                    Field f = fields.getOrDefault(member, null);
                    if (f != null) {
                        primaryKeyMemebers.append(Util.getColName(f));
                    } else {
                        primaryKeyMemebers.append(member);
                    }
                    sperator = ", ";
                }
                if (primaryKeyMemebers.length() > 0) {
                    sql.append(", PRIMARY KEY (").append(primaryKeyMemebers).append(")");
                }
            }
            if (tableIndexList != null && tableIndexList.length > 0) {
                for (TableIndex ti : tableIndexList) {
                    StringBuilder indexMemebers = new StringBuilder();
                    sperator = "";
                    for (String member : ti.members()) {
                        indexMemebers.append(sperator);
                        Field f = fields.getOrDefault(member, null);
                        if (f != null) {
                            indexMemebers.append(Util.getColName(f));
                        } else {
                            indexMemebers.append(member);
                        }
                        sperator = ", ";
                    }
                    if (ti.isUnique()) {
                        sql.append(", UNIQUE KEY ").append(ti.name()).append(" (").append(indexMemebers).append(")");
                    } else {
                        sql.append(", KEY ").append(ti.name()).append(" (").append(indexMemebers).append(")");
                    }
                }
            }
            sql.append(") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci");

            logger.info("[DBService]: create table:{}", sql.toString());
            try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
                stmt.execute();
            }
        }

        private String makeFieldSql(Field f) {
            Column ann = f.getAnnotation(Column.class);
            StringBuilder sql = new StringBuilder();
            String colName = Util.getColName(f);
            String colType = Util.getColType(f);
            sql.append(colName).append(" ").append(colType);
            int colLen = ann.len();
            if (colLen != 0) {
                if (colType.equalsIgnoreCase("float") || colType.equalsIgnoreCase("double") || colType.equalsIgnoreCase("decimal")) {
                    sql.append(" (").append(ann.len()).append(", ").append(ann.precision()).append(") ");
                } else {
                    sql.append(" (").append(ann.len()).append(") ");
                }
            }

            if (ann.charSens()) {
                sql.append(" CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ");
            }

            if (!ann.isNull()) {
                sql.append(" NOT NULL ");
            }

            if (ann.hasDefault()) {
                if (ann.defaultValue().equals("null")) {
                    sql.append(" DEFAULT NULL");
                } else {
                    sql.append(" DEFAULT '").append(ann.defaultValue()).append("'");
                }
            }

            if (ann.autoIncrement()) {
                sql.append(" AUTO_INCREMENT ");
            }

            if (ann.comment() != null && !ann.comment().equals("")) {
                sql.append(" COMMENT '").append(ann.comment()).append("'");
            }
            return sql.toString();
        }

        private void getField(Class<?> c, List<Field> l) {
            Class<?> sc = c.getSuperclass();
            if (sc != null) {
                getField(sc, l);
            }
            for (Field f : c.getDeclaredFields())
                l.add(f);
        }
    }
}
