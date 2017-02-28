package com.alibaba.rocketmq.broker.transaction.jdbc;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.transaction.TransactionRecord;
import com.alibaba.rocketmq.broker.transaction.TransactionStore;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;

public class JDBCTransactionStore implements TransactionStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);
    private final JDBCTransactionStoreConfig jdbcTransactionStoreConfig;
    private Connection connection;
    private AtomicLong totalRecordsValue = new AtomicLong(0);

    public JDBCTransactionStore(JDBCTransactionStoreConfig jdbcTransactionStoreConfig) {
        this.jdbcTransactionStoreConfig = jdbcTransactionStoreConfig;
    }

    private boolean loadDriver() {
        try {
            Class.forName(this.jdbcTransactionStoreConfig.getJdbcDriverClass()).newInstance();
            log.info("Loaded the appropriate driver, {}",
                this.jdbcTransactionStoreConfig.getJdbcDriverClass());
            return true;
        } catch (Exception e) {
            log.info("Loaded the appropriate driver Exception", e);
        }

        return false;
    }

    private boolean computeTotalRecords() {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            final String sql = "select count(offset) as total from t_transaction WHERE brokerName = ?";
            statement = this.connection.prepareStatement(sql);
            statement.setString(1, jdbcTransactionStoreConfig.getBrokerName());
            resultSet = statement.executeQuery();
            if (!resultSet.next()) {
                log.warn("computeTotalRecords ResultSet is empty");
                return false;
            }

            this.totalRecordsValue.set(resultSet.getLong(1));
        } catch (Exception e) {
            log.warn("computeTotalRecords Exception", e);
            return false;
        } finally {
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }

            if (null != resultSet) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                }
            }
        }

        return true;
    }

    private String createTableSql() {
        URL resource = JDBCTransactionStore.class.getClassLoader().getResource("transaction.sql");
        String fileContent = MixAll.file2String(resource);
        return fileContent;
    }

    private boolean createDB() {
        Statement statement = null;
        try {
            statement = this.connection.createStatement();

            String sql = this.createTableSql();
            log.info("createDB SQL:\n {}", sql);
            statement.execute(sql);
            this.connection.commit();
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
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

                // 如果表不存在，尝试初始化表
                if (!this.computeTotalRecords()) {
                    return this.createDB();
                }

                return true;
            } catch (SQLException e) {
                log.info("Create JDBC Connection Exception", e);
            }
        }

        return false;
    }

    @Override
    public void close() {
        try {
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (SQLException e) {
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
            statement = this.connection.prepareStatement("DELETE FROM t_transaction WHERE offset = ? AND brokerName = ?");
            for (long pk : pks) {
                statement.setLong(1, pk);
                statement.setString(2, jdbcTransactionStoreConfig.getBrokerName());
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            this.connection.commit();
            long updatedRows = updatedRows(executeBatch);
            totalRecordsValue.addAndGet(-updatedRows);
        } catch (Exception e) {
            log.warn("createDB Exception", e);
        } finally {
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public List<TransactionRecord> traverse(final long pk, int nums) {
        PreparedStatement statement = null;
        try {
            statement = this.connection.prepareStatement("SELECT offset, producerGroup " +
                "FROM t_transaction " +
                "WHERE brokerName = ? AND offset >= ? ORDER BY offset ASC LIMIT ?");
            statement.setString(1, jdbcTransactionStoreConfig.getBrokerName());
            statement.setLong(2, pk);
            statement.setInt(3, nums);

            ResultSet resultSet = statement.executeQuery();
            List<TransactionRecord> records = new ArrayList<>(nums);
            while (resultSet.next()) {
                TransactionRecord record = new TransactionRecord();
                record.setProducerGroup(resultSet.getString("producerGroup"));
                record.setOffset(resultSet.getLong("offset"));
                records.add(record);
            }
            return records;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
        } finally {
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
        return null;
    }

    @Override
    public long totalRecords() {
        return this.totalRecordsValue.get();
    }

    @Override
    public long minPK() {
        PreparedStatement statement = null;
        try {
            final String sql = "SELECT min(offset) FROM t_transaction WHERE brokerName = ?";
            statement = this.connection.prepareStatement(sql);
            statement.setString(1, jdbcTransactionStoreConfig.getBrokerName());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (SQLException e) {
            log.warn("Query minPK errs", e);
        } finally {
            closeQuietly(statement);
        }

        throw new RuntimeException("Failed to query minPK");
    }

    @Override
    public long maxPK() {
        PreparedStatement statement = null;
        try {
            final String sql = "SELECT max(offset) FROM t_transaction WHERE brokerName = ?";
            statement = this.connection.prepareStatement(sql);
            statement.setString(1, jdbcTransactionStoreConfig.getBrokerName());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (SQLException e) {
            log.warn("Query maxPK errs", e);
        } finally {
            closeQuietly(statement);
        }

        throw new RuntimeException("Failed to query maxPK");
    }

    @Override
    public boolean put(List<TransactionRecord> trs) {
        PreparedStatement statement = null;
        try {
            this.connection.setAutoCommit(false);
            statement = this.connection.prepareStatement("insert into t_transaction (offset, producerGroup, brokerName) values (?, ?, ?)");
            for (TransactionRecord tr : trs) {
                statement.setLong(1, tr.getOffset());
                statement.setString(2, tr.getProducerGroup());
                statement.setString(3, jdbcTransactionStoreConfig.getBrokerName());
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            this.connection.commit();
            this.totalRecordsValue.addAndGet(updatedRows(executeBatch));
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            closeQuietly(statement);
        }
    }

    private void closeQuietly(Statement statement) {
        if (null != statement) {
            try {
                statement.close();
            } catch (SQLException ignore) {
            }
        }
    }
}
