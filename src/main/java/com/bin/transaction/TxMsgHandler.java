package com.bin.transaction;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;

import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class TxMsgHandler {

    private static final Logger logger = LoggerFactory.getLogger(TxMsgHandler.class);

    /**
     * ：100ms
     */
    private static final int DEF_TIMEOUT_MS = 100;

    /**
     * 处理间隔数组(次)
     */
    private static final int[] TIMEOUT_DATA = new int[]{3, 10, 50, 100};

    /**
     * 最大处理次数
     */
    private static final int MAX_DEAL_TIME = 3;
    // 发送失败的数据 再次处理
    private static final int secondsPeriod = 120;

    private static final int deleteTxMsgBatch = 200;

    public static final int deleteTimePeriod = 180 * 1000;

    private static final int LIMIT_NUM = 50;
    private static final int MAX_DEAL_NUM_ONE_TIME = 2000;

    private static final int max_blank = 10;

    private static final int closeWaitTime = 5000;

    private final DefaultMQProducer producer;

    private final PriorityBlockingQueue<TxMsg> txMsgQueue;


    /**
     * 定时线程池：其他线程
     */
    private final ScheduledExecutorService scheService;

    private final TxMsgSqlStore txMsgSqlStore;




    private long lastDelete = System.currentTimeMillis();

    private final DistributedLock distributedLock;

    private final Object lock = new Object();

    private final AtomicBoolean txMsgDeliveryRun = new AtomicBoolean(false);
    final AtomicBoolean  state = new AtomicBoolean(true);

    public TxMsgHandler(DefaultMQProducer producer, TxMsgSqlStore txMsgSqlStore,
                        DistributedLock distributedLock) {
        this.producer = producer;
        this.distributedLock = distributedLock;
        this.txMsgSqlStore = txMsgSqlStore;


        txMsgQueue = new PriorityBlockingQueue<>(1000, new Comparator<TxMsg>() {
            @Override
            public int compare(TxMsg o1, TxMsg o2) {
                long diff = o1.getCreateTime() - o2.getCreateTime();
                if (diff > 0) {
                    return 1;
                } else if (diff < 0) {
                    return -1;
                }
                return 0;
            }
        });


        scheService = Executors.newScheduledThreadPool(1, (run) -> {
            Thread thread = defaultThreadFactory().newThread(run);
            thread.setName("TxMsgHandlerScheduled-" + run.getClass().getSimpleName());
            return thread;
        });

        scheService.scheduleAtFixedRate(new HistoryTxMsgOperation(), secondsPeriod, secondsPeriod, TimeUnit.SECONDS);

    }


    public void shutdown() {
        state.compareAndSet(true, false);
        try {
            scheService.awaitTermination(closeWaitTime, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {}

        if (!scheService.isShutdown()) {
            scheService.shutdownNow();
        }

        logger.info("transaction Msg Handler close");
    }

    // 同步处理导致请求时间变长
    public void sendMsg(TxMsgModel txMsgModel) {
        Message message = buildMessage(txMsgModel);
        try {
            SendResult send = producer.send(message);
            logger.info("msgId {} topic {} tag {} sendMsg result {}",
                    txMsgModel.getId(), message.getTopic(), message.getTags(), send);
            if (null == send || send.getSendStatus() != SendStatus.SEND_OK) {
                TxMsg txMsg = new TxMsg(txMsgModel.getId());
                txMsg.setNextExpireTime(TIMEOUT_DATA[0]);
                txMsg.setTxMsgModel(txMsgModel);
                txMsgQueue.put(txMsg);
                logger.debug("put msg in timeWheelQueue {} ", txMsg);
                if (!txMsgDeliveryRun.get()) {
                    Thread thread = new Thread(new TxMsgDeliveryOperation());
                    thread.setName("TxMsgDelivery");
                    thread.start();
                    txMsgDeliveryRun.lazySet(true);
                }
            } else if (send.getSendStatus() == SendStatus.SEND_OK) {
                // 投递成功，修改数据库的状态(标识已提交)
                int res = txMsgSqlStore.updateSendMsg(txMsgModel);
                logger.debug("msgId {} updateMsgStatus success res {}", txMsgModel.getId(), res);
            }
        } catch (Exception e) {
            logger.error("txMsgHandler sendMsg fail", e);
        }

    }

    class TxMsgDeliveryOperation implements Runnable {

        @Override
        public void run() {

            int blank = 0;
            while (state.get() && blank <= max_blank) {
                TxMsg txMsg = null;
                try {
                    txMsg = txMsgQueue.poll(DEF_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                }
                long cruTime = System.currentTimeMillis();
                if (txMsg == null) {
                    deleteSendedTxMsg(cruTime);
                    ++blank;
                    continue;
                }

                if (txMsg.getNextExpireTime() > cruTime) {
                    txMsg.setCreateTime(cruTime);
                    txMsgQueue.put(txMsg);
                    continue;
                }

                TxMsgModel txMsgModel = txMsg.getTxMsgModel();
                if (txMsgModel == null)
                    txMsgModel = txMsgSqlStore.getTxMsg(txMsg);
                if (txMsgModel == null)
                    continue;
                int doTimes = txMsg.incDoTimes();
                Message message = buildMessage(txMsgModel);
                try {
                    SendResult send = producer.send(message);
                    logger.info("msgId {} topic {} tag {} sendMsg result {}",
                            txMsgModel.getId(), message.getTopic(), message.getTags(), send);
                    if (null == send || send.getSendStatus() != SendStatus.SEND_OK) {
                        if (doTimes < MAX_DEAL_TIME) {
                            txMsg.setNextExpireTime(TIMEOUT_DATA[doTimes]);
                            txMsg.setCreateTime(cruTime);
                            txMsgQueue.put(txMsg);
                            logger.debug("put msg in timeWheelQueue {} ", txMsg);
                        }
                    } else if (send.getSendStatus() == SendStatus.SEND_OK) {
                        int res = txMsgSqlStore.updateSendMsg(txMsgModel);
                        logger.debug("msgId {} updateMsgStatus success res {}", txMsgModel.getId(), res);
                    }
                } catch (Exception e) {
                    logger.error(" tx msg {} send  fail", txMsgModel, e);
                }
            }
            txMsgDeliveryRun.lazySet(false);
        }
    }


    public static Message buildMessage(TxMsgModel txMsgModel) {
        String topic = txMsgModel.getTopic();
        String tag = txMsgModel.getTag();
        String content = txMsgModel.getContent();
        String id = txMsgModel.getId() + "";

        Message msg = new Message(topic, tag, id, content.getBytes(StandardCharsets.UTF_8));
        String header = String.format("{\"topic\":\"%s\",\"tag\":\"%s\",\"id\":\"%s\",\"createTime\":\"%s\"}", topic, tag, id, System.currentTimeMillis());
        msg.putUserProperty("MQheader", header);
        return msg;
    }

    private void deleteSendedTxMsg(long cruTime) {
        if (cruTime > lastDelete + deleteTimePeriod)
            try {
                // 删除不怕并发问题
                txMsgSqlStore.deleteSendedTxMsg(deleteTxMsgBatch);
                lastDelete = cruTime;
            } catch (Exception e) {
                logger.error("deleteSendedTxMsg fail", e);
            }
    }

    class HistoryTxMsgOperation implements Runnable {

        @Override
        public void run() {
            if (state.get()) {

                if (distributedLock.isLock()) {
                    int count = 0;
                    int num = LIMIT_NUM;
                    while (num == LIMIT_NUM && count < MAX_DEAL_NUM_ONE_TIME) {
                        List<TxMsgModel> history = txMsgSqlStore.getHistory(LIMIT_NUM);
                        num = history.size();
                        count += num;

                        for (TxMsgModel txMsgModel : history) {
                            try {
                                Message message = buildMessage(txMsgModel);

                                SendResult send = producer.send(message);
                                logger.info("msgId {} topic {} tag {} sendTxMsg result {}", txMsgModel.getId(), message.getTopic(), message.getTags(), send);
                                if (send != null && send.getSendStatus() == SendStatus.SEND_OK) {
                                    // 修改数据库的状态
                                    int res = txMsgSqlStore.updateSendMsg(txMsgModel);
                                    logger.debug("msgId {} updateMsgStatus success res {}", txMsgModel.getId(), res);
                                }
                            } catch (Exception e) {
                                logger.error("HistoryTxMsg do fail", e);
                            }
                        }
                    }
                }
                deleteSendedTxMsg(System.currentTimeMillis());
            }
        }
    }
}
