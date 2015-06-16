package com.alibaba.rocketmq.store.transaction.jdbc;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import com.alibaba.rocketmq.store.transaction.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class JDBCTransactionStore implements TransactionStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);
    private final JDBCTransactionStoreConfig jdbcTransactionStoreConfig;
    private Connection connection;
    private AtomicLong totalRecordsValue = new AtomicLong(0);

    private ConcurrentHashMap<String, Integer> producerGroupIdMap = new ConcurrentHashMap<String, Integer>();

    /**
     * SQL to check existence of specified table.
     */
    private static final String SQL_TABLE_EXISTS = "SELECT count(1) > 1 AS table_exists " +
            "FROM information_schema.TABLES " +
            "WHERE TABLE_NAME = ? and TABLE_SCHEMA= ?";

    private static final String SQL_CREATE_TRANSACTION_TABLE = "CREATE TABLE IF NOT EXISTS t_transaction (" +
            "offset NUMERIC(20) NOT NULL PRIMARY KEY, " +
            "producer_group_id INT NOT NULL," +
            "create_time TIME NOT NULL)";

    private static final String SQL_CREATE_PRODUCER_GROUP_TABLE = "CREATE TABLE IF NOT EXISTS t_producer_group (" +
            "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
            "name VARCHAR(64) NOT NULL, " +
            "CONSTRAINT uniq_producer_group_name UNIQUE(name))";


    /**
     * TODO: This SQL uses NOW(), stopping MySQL from using cache. Need optimization.
     */
    private static final String SQL_QUERY_LAGGED_TRANSACTION_RECORDS = "SELECT * FROM t_transaction WHERE create_time < NOW() - INTERVAL 30 SECOND";


    private static final String SQL_INSERT_PRODUCER_GROUP =
            "INSERT INTO t_producer_group (id, name) VALUES (NULL, ?)";

    private static final String SQL_GET_PRODUCER_GROUP_ID = "SELECT name, id FROM t_producer_group";

    public String getProducerGroup(int producerGroupId) {

        if (producerGroupId < 0) {
            return null;
        }

        for (Map.Entry<String, Integer> next : producerGroupIdMap.entrySet()) {
            if (next.getValue() == producerGroupId) {
                return next.getKey();
            }
        }
        return null;
    }

    public int getOrCreateProducerGroupId(String producerGroup) {

        if (null == producerGroup || producerGroup.isEmpty()) {
            return -1;
        }

        if (!producerGroupIdMap.containsKey(producerGroup)) {
            insertProducerGroup(producerGroup);
        }

        return producerGroupIdMap.get(producerGroup);
    }

    public JDBCTransactionStore(JDBCTransactionStoreConfig jdbcTransactionStoreConfig) {
        this.jdbcTransactionStoreConfig = jdbcTransactionStoreConfig;
    }

    private boolean loadDriver() {
        try {
            Class.forName(this.jdbcTransactionStoreConfig.getJdbcDriverClass()).newInstance();
            log.info("Loaded the appropriate driver, {}",
                this.jdbcTransactionStoreConfig.getJdbcDriverClass());
            return true;
        }
        catch (Exception e) {
            log.info("Loaded the appropriate driver Exception", e);
        }

        return false;
    }


    private boolean computeTotalRecords() {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = this.connection.createStatement();

            resultSet = statement.executeQuery("select count(offset) as total from t_transaction");
            if (!resultSet.next()) {
                log.warn("computeTotalRecords ResultSet is empty");
                return false;
            }

            this.totalRecordsValue.set(resultSet.getLong(1));
        }
        catch (Exception e) {
            log.warn("computeTotalRecords Exception", e);
            return false;
        }
        finally {
            if (null != statement) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                }
            }

            if (null != resultSet) {
                try {
                    resultSet.close();
                }
                catch (SQLException e) {
                }
            }
        }

        return true;
    }

    /**
     * This method checks if the given tables exists in DB.
     * @param tableName table name to check.
     * @return true if exists; false otherwise.
     * @throws SQLException If any error occurs.
     */
    private boolean tableExists(String tableName) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_TABLE_EXISTS);
        preparedStatement.setString(1, tableName);
        preparedStatement.setString(2, connection.getSchema());
        ResultSet resultSet = preparedStatement.executeQuery();

        if (resultSet.first()) {
            boolean exists = resultSet.getBoolean("table_exists");
            preparedStatement.close();
            return exists;
        }

        return false;
    }


    private boolean createTable(String tableName, String sql) {
        Statement statement = null;
        try {
            statement = this.connection.createStatement();
            log.info("create table: {} \n SQL: {}", tableName, sql);
            statement.execute(sql);
            this.connection.commit();
            return true;
        } catch (Exception e) {
            log.warn("create table: {}  Exception", tableName, e);
            return false;
        } finally {
            if (null != statement) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                }
            }
        }
    }

    public boolean insertProducerGroup(String producerGroup) {
        if (!producerGroupIdMap.containsKey(producerGroup)) {
            if (null != connection) {
                try {
                    PreparedStatement preparedStatement = connection.prepareStatement(SQL_INSERT_PRODUCER_GROUP, Statement.RETURN_GENERATED_KEYS);
                    preparedStatement.setString(1, producerGroup);
                    preparedStatement.executeUpdate();
                    ResultSet resultSet = preparedStatement.getGeneratedKeys();
                    resultSet.first();
                    int groupId = resultSet.getInt(1);
                    connection.commit();
                    producerGroupIdMap.putIfAbsent(producerGroup, groupId);
                    preparedStatement.close();
                    return true;
                } catch (SQLException e) {
                    log.error("Failed to insert new producer group: {}", producerGroup);
                }
            }
        }

        return false;
    }


    @Override
    public boolean open() {
        if (this.loadDriver()) {
            Properties props = new Properties();
            props.put("user", jdbcTransactionStoreConfig.getJdbcUser());
            props.put("password", jdbcTransactionStoreConfig.getJdbcPassword());

            try {
                this.connection = DriverManager.getConnection(this.jdbcTransactionStoreConfig.getJdbcURL(), props);
                this.connection.setAutoCommit(false);

                boolean success = true;
                // 如果表不存在，尝试初始化表
                if (!tableExists("t_transaction")) {
                    success = success && createTable("t_transaction", SQL_CREATE_TRANSACTION_TABLE);
                }

                if (!tableExists("t_producer_group")) {
                    success = success && createTable("t_producer_group", SQL_CREATE_PRODUCER_GROUP_TABLE);
                }

                //pre-fetch all existing producer group ID mapping.
                loadProducerGroupIdMapFromDB();

                return success;
            } catch (SQLException e) {
                log.info("Create JDBC Connection Exception", e);
            }
        }

        return false;
    }

    private boolean loadProducerGroupIdMapFromDB() {
        boolean isSuccess = true;
        if (null != connection) {
            try {
                Statement statement = connection.createStatement();
                log.info("Executing SQL: {}", SQL_GET_PRODUCER_GROUP_ID);
                ResultSet resultSet = statement.executeQuery(SQL_GET_PRODUCER_GROUP_ID);
                while (resultSet.next()) {
                    producerGroupIdMap.putIfAbsent(resultSet.getString("name"), resultSet.getInt("id"));
                }
                statement.close();
            } catch (SQLException e) {
                log.error("Query Producer Group Name-ID mapping error", e);
            }
        } else {
            isSuccess = false;
        }

        return isSuccess;
    }


    @Override
    public void close() {
        try {
            if (this.connection != null) {
                this.connection.close();
            }
        }
        catch (SQLException e) {
        }
    }


    private long updatedRows(int[] rows) {
        long res = 0;
        for (int i : rows) {
            res += i;
        }

        return res;
    }


    @Override
    public void remove(List<Long> pks) {
        PreparedStatement statement = null;
        try {
            this.connection.setAutoCommit(false);
            statement = this.connection.prepareStatement("DELETE FROM t_transaction WHERE offset = ?");
            for (long pk : pks) {
                statement.setLong(1, pk);
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            System.out.println(Arrays.toString(executeBatch));
            this.connection.commit();
        }
        catch (Exception e) {
            log.warn("createDB Exception", e);
        }
        finally {
            if (null != statement) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                }
            }
        }
    }


    @Override
    public List<TransactionRecord> traverse(long pk, int nums) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public long totalRecords() {
        // TODO Auto-generated method stub
        return this.totalRecordsValue.get();
    }


    @Override
    public long minPK() {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long maxPK() {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public boolean put(List<TransactionRecord> trs) {

        for (TransactionRecord tr: trs) {
            if (!producerGroupIdMap.containsKey(tr.getProducerGroup())) {
                insertProducerGroup(tr.getProducerGroup());
            }
        }

        PreparedStatement statement = null;
        try {
            this.connection.setAutoCommit(false);
            statement = this.connection.prepareStatement("insert into t_transaction values (?, ?)");
            for (TransactionRecord tr : trs) {
                statement.setLong(1, tr.getOffset());
                statement.setInt(2, producerGroupIdMap.get(tr.getProducerGroup()));
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            this.connection.commit();
            this.totalRecordsValue.addAndGet(updatedRows(executeBatch));
            return true;
        }
        catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        }
        finally {
            if (null != statement) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                }
            }
        }
    }

    public Map<String, Set<Long>> getLaggedTransaction() {
        if (null != connection) {
            Statement statement = null;
            try {
                statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(SQL_QUERY_LAGGED_TRANSACTION_RECORDS);
                Map<String, Set<Long>> result = new HashMap<String, Set<Long>>();
                while (resultSet.next()) {
                    String producerGroup = getProducerGroup(resultSet.getInt("producer_group_id"));
                    if (result.containsKey(producerGroup)) {
                        result.get(producerGroup).add(resultSet.getLong("offset"));
                    } else {
                        Set<Long> offsets = new HashSet<Long>();
                        result.put(producerGroup, offsets);
                        offsets.add(resultSet.getLong("offset"));
                    }
                }
                return result;
            } catch (SQLException e) {
                log.error("Failed to create statement.", e);
            } finally {
                if (null != statement) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        log.error("Error while closing statement", e);
                    }
                }
            }
        }

        return null;
    }
}
