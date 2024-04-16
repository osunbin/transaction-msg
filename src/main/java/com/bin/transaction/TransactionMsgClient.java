package com.bin.transaction;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static com.bin.transaction.DistributedLock.EMPTY;


public class TransactionMsgClient {

    protected static final Logger logger = LoggerFactory.getLogger(TransactionMsgClient.class);


    private final TxMsgSqlStore txMsgSqlStore;

    private final TxMsgHandler txMsgHandler;

    private final DataSource dataSource;


    public TransactionMsgClient(DefaultMQProducer producer, DataSource dataSource) {
        txMsgSqlStore = new TxMsgSqlStore(dataSource);
        txMsgHandler = new TxMsgHandler(producer, txMsgSqlStore, EMPTY);
        this.dataSource = dataSource;
    }

    public TransactionMsgClient(DefaultMQProducer producer, DataSource dataSource,
                                DistributedLock distributedLock) {
        txMsgSqlStore = new TxMsgSqlStore(dataSource);
        txMsgHandler = new TxMsgHandler(producer, txMsgSqlStore, distributedLock);
        this.dataSource = dataSource;
    }


    public void shutdown() {
        txMsgHandler.shutdown();
        logger.info("transaction msg client close ");
    }

    public Long sendTxMsg(String content, String topic, String tag) throws SQLException {
        TxMsgModel txMsg = null;
        try {

            Connection connection = DataSourceUtils.getConnection(dataSource);
            txMsg = sendTxMsg(connection, content, topic, tag);
            addCallback(txMsg);
        } catch (Exception ex) {
            logger.error("sendMsg fail topic {} tag {} ", topic, tag, ex);
            if (ex instanceof SQLException) {
                SQLException sql = (SQLException) ex;
                throw sql;
            } else {
                throw new RuntimeException(ex);
            }
        }
        return txMsg.getId();
    }


    public TxMsgModel sendTxMsg(Connection conn, String content, String topic, String tag) throws Exception {

        if (content == null || content.isEmpty() || topic == null || topic.isEmpty()) {
            logger.error("content or topic is null or empty");
            throw new RuntimeException("content or topic is null or empty, notice ");
        }

        logger.debug("insert to msgTable topic {} tag {} Connection {} Autocommit {} ", topic, tag, conn, conn.getAutoCommit());
        if (conn.getAutoCommit()) {
            logger.error("attention not in transaction topic {} tag {} Connection {} Autocommit {} ", topic, tag, conn, conn.getAutoCommit());
            throw new RuntimeException("connection not in transaction conn " + conn);
        }

        TxMsgModel txMsg = null;
        try {
            txMsg = txMsgSqlStore.insertTxMsg(conn, content, topic, tag);
        } catch (SQLException ex) {
            logger.error("sendMsg fail topic {} tag {} ", topic, tag, ex);
            throw ex;
        }
        return txMsg;
    }


    private void addCallback(TxMsgModel txMsg) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            // 添加监听器，在事务提交后触发后续任务
            TransactionSynchronization transactionSynchronization = new TransactionSynchronizationAdapter() {
                @Override
                public void afterCommit() {
                    sendMsg(txMsg);

                }
            };
            TransactionSynchronizationManager.registerSynchronization(transactionSynchronization);
        }
    }

    public void sendMsg(TxMsgModel txMsg) {
        txMsgHandler.sendMsg(txMsg);
    }


}
