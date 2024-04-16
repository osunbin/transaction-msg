package com.bin.transaction;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class TxMsgSqlStore {


    private static final Logger logger = LoggerFactory.getLogger(TxMsgSqlStore.class);

    /**
     * 事务消息状态-等待
     */
    private static final int MSG_STATUS_WAITING = 1;

    /**
     * 事务消息状态-发送
     */
    private static final int MSG_STATUS_SEND = 2;


    private static final long dayTimeDiff = 3 * 1000 * 60 * 60 * 24;

    private static int minuteTimeDiff = 1000 * 60 * 10;


    /**
     * 默认表明
     */
    private static String tableName = "mq_messages";



    /**
     * insert sql
     */
    private static String insertSQL = "insert into " + tableName + "(content,topic,tag,status,create_time) values(?,?,?,?,?)";

    /**
     * select sql
     */
    private static String selectByIdSQL = "select id,content,topic,tag,status,create_time from " + tableName + " where id= :id";


    /**
     * 更改事务消息状态
     */
    private static String updateStatusSQL = "update " + tableName + " set status= :status where id= :id ";

    /**
     * 获取 等待 事务消息列表
     */
    private static String selectWaitingMsgSQL = "select id,content,topic,tag,status,create_time from " + tableName + " where status= :status and create_time >= :createTime order by id limit :limit";


    /**
     * delete sql
     */
    private static String deleteMsgSQL = "delete from " + tableName + " where status= :status and create_time <= :createTime  limit :limit ";



    private Jdbi internalJdbi;

    TxMsgMapper txMsgMapper = new TxMsgMapper();

    public TxMsgSqlStore(DataSource ds) {
        internalJdbi = Jdbi.create(ds);
    }



    public TxMsgModel insertTxMsg(Connection conn, String content,
                             String topic, String tag) throws SQLException {
        PreparedStatement psmt = null;
        ResultSet results = null;
        try {
            // Statement.RETURN_GENERATED_KEYS   得到插入字段的主键Id , 但前提表中Id必须是自增长的.
            psmt = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
            psmt.setString(1, content);
            psmt.setString(2, topic);
            psmt.setString(3, tag);
            psmt.setInt(4, MSG_STATUS_WAITING);
            psmt.setLong(5,System.currentTimeMillis());
            psmt.executeUpdate();
            results = psmt.getGeneratedKeys();
            Long id = null;
            if (results.next()) {
                id = results.getLong(1);
            }
            TxMsgModel txMsgModel = new TxMsgModel();
            txMsgModel.setId(id);
            txMsgModel.setTopic(topic);
            txMsgModel.setTag(tag);
            txMsgModel.setContent(content);
            return txMsgModel;
        }catch (SQLException ex) {
            throw ex;
        }finally {
            if (results != null) {
                try {
                    results.close();
                } catch (SQLException e) {
                    logger.error("close Connection ResultSet error {} ", results, e);
                }
            }
            if (psmt != null) {
                try {
                    psmt.close();
                } catch (SQLException e) {
                    logger.error("close Connection PreparedStatement {} error ", psmt, e);
                }
            }
        }

    }

    public TxMsgModel getTxMsg(TxMsg txMsg) {
        TxMsgModel txMsgModel = internalJdbi.withHandle((handle) -> {
            return handle.createQuery(selectByIdSQL)
                    .bind("id", txMsg.getId())
                    .map(txMsgMapper).findOnly();
        });

        return txMsgModel;
    }

    public int updateSendMsg(TxMsgModel txMsgModel) {
        int integer = internalJdbi.withHandle((handle) -> {
            return handle.createUpdate(updateStatusSQL)
                    .bind("status", MSG_STATUS_SEND)
                    .bind("id", txMsgModel.getId())
                    .execute();
        });
        return integer;
    }

    public List<TxMsgModel> getHistory(int pageSize) {
        long time = System.currentTimeMillis() - minuteTimeDiff;
        List<TxMsgModel> txMsgModels = internalJdbi.withHandle((handle) -> {
            return handle.createQuery(selectWaitingMsgSQL)
                    .bind("status", MSG_STATUS_WAITING)
                    .bind("createTime", time)
                    .bind("limit", pageSize)
                    .map(txMsgMapper).list();

        });
        return txMsgModels;
    }

    public int deleteSendedTxMsg(int pageSize) {
        long time = System.currentTimeMillis() - dayTimeDiff;
       return internalJdbi.withHandle((handle) ->{
          return   handle.createUpdate(deleteMsgSQL)
                    .bind("status",MSG_STATUS_SEND)
                    .bind("createTime",time)
                    .bind("limit",pageSize)
                    .execute();
        });
    }





    class TxMsgMapper implements RowMapper<TxMsgModel> {

        // id,content,topic,tag,status,create_time,delay
        @Override
        public TxMsgModel map(ResultSet rs, StatementContext ctx) throws SQLException {
            TxMsgModel txMsgModel = new TxMsgModel();
            txMsgModel.setId(Long.valueOf(rs.getString("id")));
            txMsgModel.setContent(rs.getString("content"));
            txMsgModel.setTopic(rs.getString("topic"));
            txMsgModel.setTag(rs.getString("tag"));
            txMsgModel.setStatus(rs.getInt("status"));
            txMsgModel.setCreateTime(rs.getLong("create_time"));
            return txMsgModel;
        }
    }
}
