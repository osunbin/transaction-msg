package com.bin.transaction;

import java.util.Date;

public class TxMsgModel {

    /**
     * 主键
     */
    private Long id;

    /**
     * 事务消息
     */
    private String content;

    /**
     * 主题
     */
    private String topic;

    /**
     * 标签
     */
    private String tag;

    /**
     * 状态：1-等待，2-发送
     */
    private int status;

    /**
     * 创建时间
     */
    private long createTime;



    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }


    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("TxMsgModel{");
        sb.append("id=").append(id);
        sb.append(", content='").append(content).append('\'');
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append(", status=").append(status);
        sb.append(", createTime=").append(createTime);
        sb.append('}');
        return sb.toString();
    }
}
