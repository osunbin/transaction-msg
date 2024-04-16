package com.bin.transaction;

import java.util.Objects;

public class TxMsg {

    /**
     * 主键
     */
    private Long id;

    /**
     * 已经处理次数
     */
    private int doTimes;

    /**
     * 创建时间
     */
    private long createTime;

    /**
     * 下次超时时间
     */
    private long nextExpireTime = 0;

    private TxMsgModel txMsgModel;

    public TxMsg(Long id) {
        this.id = id;
        this.doTimes = 0;
        this.createTime = System.currentTimeMillis();
    }

    public Long getId() {
        return id;
    }


    public int incDoTimes() {
        return  ++doTimes;
    }

    public int getDoTimes() {
        return doTimes;
    }

    public void setDoTimes(int doTimes) {
        this.doTimes = doTimes;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getNextExpireTime() {
        return nextExpireTime;
    }

    public void setNextExpireTime(long nextExpireTime) {
        this.nextExpireTime = System.currentTimeMillis() + nextExpireTime;
    }

    public TxMsgModel getTxMsgModel() {
        return txMsgModel;
    }

    public void setTxMsgModel(TxMsgModel txMsgModel) {
        this.txMsgModel = txMsgModel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxMsg txMsg = (TxMsg) o;
        return doTimes == txMsg.doTimes && createTime == txMsg.createTime && Objects.equals(id, txMsg.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, doTimes, createTime);
    }
}
