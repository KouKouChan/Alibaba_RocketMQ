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
import java.sql.Timestamp;
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
            "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)";

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

    private static final String SQL_INSERT_TRANSACTION_RECORD = "INSERT INTO t_transaction(offset, producer_group_id, create_time) VALUES (?, ?, ?)";

    private static final String SQL_DELETE_TRANSACTION_RECORD = "DELETE FROM t_transaction WHERE offset = ?";

    private static final String SQL_TOTAL_TRANSACTION_RECORD = "SELECT COUNT(1) AS total FROM t_transaction";

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

    /**
     * This method checks if the given tables exists in DB.
     * @param tableName table name to check.
     * @return true if exists; false otherwise.
     * @throws SQLException If any error occurs.
     */
    private boolean tableExists(String tableName) {
        if (null == connection) {
            return false;
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(SQL_TABLE_EXISTS);
            preparedStatement.setString(1, tableName);
            preparedStatement.setString(2, connection.getSchema());
            resultSet = preparedStatement.executeQuery();

            if (resultSet.first()) {
                boolean exists = resultSet.getBoolean("table_exists");
                preparedStatement.close();
                return exists;
            }

            return false;
        } catch (SQLException e) {
            log.error("DB error: failed to create table.", e);
            return false;
        } finally {
            close(resultSet);
            close(preparedStatement);
        }
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
            close(statement);
        }
    }

    public boolean insertProducerGroup(String producerGroup) {
        if (!producerGroupIdMap.containsKey(producerGroup)) {
            if (null != connection) {
                PreparedStatement preparedStatement = null;
                ResultSet resultSet = null;
                try {
                    preparedStatement = connection.prepareStatement(SQL_INSERT_PRODUCER_GROUP, Statement.RETURN_GENERATED_KEYS);
                    preparedStatement.setString(1, producerGroup);
                    preparedStatement.executeUpdate();
                    resultSet = preparedStatement.getGeneratedKeys();
                    resultSet.first();
                    int groupId = resultSet.getInt(1);
                    connection.commit();
                    producerGroupIdMap.putIfAbsent(producerGroup, groupId);
                    return true;
                } catch (SQLException e) {
                    log.error("Failed to insert new producer group: {}", producerGroup);
                } finally {
                    close(resultSet);
                    close(preparedStatement);
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
                success = success && loadProducerGroupIdMapFromDB();
                success = success && loadTotalRecords();

                return success;
            } catch (SQLException e) {
                log.info("Create JDBC Connection Exception", e);
            }
        }

        return false;
    }

    private boolean loadTotalRecords() {
        if (null == connection) {
            return false;
        }

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(SQL_TOTAL_TRANSACTION_RECORD);
            resultSet.first();
            totalRecordsValue.set(resultSet.getLong("total"));
            return true;
        } catch (SQLException e) {
            log.error("DB error.", e);
            return false;
        } finally {
            close(resultSet);
            close(statement);
        }

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
        } catch (SQLException e) {
            log.error("Error while closing connection.", e);
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
        if (null == pks || pks.isEmpty()) {
            return;
        }

        PreparedStatement statement = null;
        try {
            this.connection.setAutoCommit(false);
            statement = this.connection.prepareStatement(SQL_DELETE_TRANSACTION_RECORD);
            for (long pk : pks) {
                statement.setLong(1, pk);
                statement.addBatch();
            }
            statement.executeBatch();
            this.connection.commit();
            totalRecordsValue.addAndGet(-1 * pks.size());
        }
        catch (Exception e) {
            log.warn("Remove Transaction Record Exception", e);
        } finally {
            close(statement);
        }
    }


    @Override
    public List<TransactionRecord> traverse(long pk, int nums) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public long totalRecords() {
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
            statement = this.connection.prepareStatement(SQL_INSERT_TRANSACTION_RECORD);
            for (TransactionRecord tr : trs) {
                statement.setLong(1, tr.getOffset());
                statement.setInt(2, producerGroupIdMap.get(tr.getProducerGroup()));
                statement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
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
        } finally {
            close(statement);
        }
    }

    public Map<String, Set<Long>> getLaggedTransaction() {
        if (null != connection) {
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                statement = connection.createStatement();
                resultSet = statement.executeQuery(SQL_QUERY_LAGGED_TRANSACTION_RECORDS);
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
                close(resultSet);
                close(statement);
            }
        }
        return null;
    }

    private static void close(Statement statement) {
        if (null != statement) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.error("Error while closing result set", e);
            }
        }
    }


    private static void close(ResultSet resultSet) {
        if (null != resultSet) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.error("Error while closing result set", e);
            }
        }
    }
}
