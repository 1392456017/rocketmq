/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.producer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class ProduceAccumulator {
    /**
     * 总缓存大小限制，默认32MB
     * 作用：控制整个累积器能够缓存的消息总大小，防止内存溢出
     * 当达到此限制时，新消息将无法加入累积器
     */
    private long totalHoldSize = 32 * 1024 * 1024;

    /**
     * 单个批次的大小限制，默认32KB
     * 作用：控制单个MessageBatch的最大大小
     * 当累积的消息达到此大小时，会触发批量发送
     */
    private long holdSize = 32 * 1024;

    /**
     * 批次等待时间，默认10毫秒
     * 作用：即使消息大小未达到holdSize，超过此时间也会触发发送
     * 平衡延迟和吞吐量：避免消息等待过久，同时允许一定的批量聚合
     */
    private int holdMs = 10;

    /**
     * 日志记录器
     */
    private final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);

    /**
     * 同步发送的守护线程服务
     * 作用：定期检查同步发送批次，唤醒等待的线程，清理空批次
     * 工作机制：每隔holdMs/2时间检查一次所有同步批次的状态
     */
    private final GuardForSyncSendService guardThreadForSyncSend;

    /**
     * 异步发送的守护线程服务
     * 作用：定期检查异步发送批次，触发达到条件的批次发送，清理空批次
     * 工作机制：每隔holdMs/2时间检查一次所有异步批次是否ready to send
     */
    private final GuardForAsyncSendService guardThreadForAsyncSend;

    /**
     * 同步发送批次缓存表
     * Key: AggregateKey - 聚合键（topic + mq + waitStoreMsgOK + tag）
     * Value: MessageAccumulation - 消息累积对象
     * 作用：按照聚合键对同步发送的消息进行分组批量处理
     */
    private final Map<AggregateKey, MessageAccumulation> syncSendBatchs = new ConcurrentHashMap<AggregateKey, MessageAccumulation>();

    /**
     * 异步发送批次缓存表
     * Key: AggregateKey - 聚合键（topic + mq + waitStoreMsgOK + tag）
     * Value: MessageAccumulation - 消息累积对象
     * 作用：按照聚合键对异步发送的消息进行分组批量处理
     */
    private final Map<AggregateKey, MessageAccumulation> asyncSendBatchs = new ConcurrentHashMap<AggregateKey, MessageAccumulation>();

    /**
     * 当前累积器持有的消息总大小（字节）
     * 作用：实时跟踪内存使用情况，与totalHoldSize配合进行流控
     * 线程安全：使用AtomicLong保证并发环境下的准确性
     */
    private final AtomicLong currentlyHoldSize = new AtomicLong(0);

    /**
     * 客户端实例名称
     * 作用：用于标识不同的客户端实例，在日志和线程命名中使用
     */
    private final String instanceName;

    public ProduceAccumulator(String instanceName) {
        this.instanceName = instanceName;
        this.guardThreadForSyncSend = new GuardForSyncSendService(this.instanceName);
        this.guardThreadForAsyncSend = new GuardForAsyncSendService(this.instanceName);
    }

    private class GuardForSyncSendService extends ServiceThread {
        private final String serviceName;

        public GuardForSyncSendService(String clientInstanceName) {
            serviceName = String.format("Client_%s_GuardForSyncSend", clientInstanceName);
        }

        @Override
        public String getServiceName() {
            return serviceName;
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.doWork();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        private void doWork() throws InterruptedException {
            Collection<MessageAccumulation> values = syncSendBatchs.values();
            final int sleepTime = Math.max(1, holdMs / 2);
            for (MessageAccumulation v : values) {
                v.wakeup();
                synchronized (v) {
                    synchronized (v.closed) {
                        if (v.messagesSize.get() == 0) {
                            v.closed.set(true);
                            syncSendBatchs.remove(v.aggregateKey, v);
                        } else {
                            v.notify();
                        }
                    }
                }
            }
            Thread.sleep(sleepTime);
        }
    }

    private class GuardForAsyncSendService extends ServiceThread {
        private final String serviceName;

        public GuardForAsyncSendService(String clientInstanceName) {
            serviceName = String.format("Client_%s_GuardForAsyncSend", clientInstanceName);
        }

        @Override
        public String getServiceName() {
            return serviceName;
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.doWork();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        private void doWork() throws Exception {
            Collection<MessageAccumulation> values = asyncSendBatchs.values();
            final int sleepTime = Math.max(1, holdMs / 2);
            for (MessageAccumulation v : values) {
                if (v.readyToSend()) {
                    v.send(null);
                }
                synchronized (v.closed) {
                    if (v.messagesSize.get() == 0) {
                        v.closed.set(true);
                        asyncSendBatchs.remove(v.aggregateKey, v);
                    }
                }
            }
            Thread.sleep(sleepTime);
        }
    }

    void start() {
        guardThreadForSyncSend.start();
        guardThreadForAsyncSend.start();
    }

    void shutdown() {
        guardThreadForSyncSend.shutdown();
        guardThreadForAsyncSend.shutdown();
    }

    int getBatchMaxDelayMs() {
        return holdMs;
    }

    void batchMaxDelayMs(int holdMs) {
        if (holdMs <= 0 || holdMs > 30 * 1000) {
            throw new IllegalArgumentException(String.format("batchMaxDelayMs expect between 1ms and 30s, but get %d!", holdMs));
        }
        this.holdMs = holdMs;
    }

    long getBatchMaxBytes() {
        return holdSize;
    }

    void batchMaxBytes(long holdSize) {
        if (holdSize <= 0 || holdSize > 2 * 1024 * 1024) {
            throw new IllegalArgumentException(String.format("batchMaxBytes expect between 1B and 2MB, but get %d!", holdSize));
        }
        this.holdSize = holdSize;
    }

    long getTotalBatchMaxBytes() {
        return holdSize;
    }

    void totalBatchMaxBytes(long totalHoldSize) {
        if (totalHoldSize <= 0) {
            throw new IllegalArgumentException(String.format("totalBatchMaxBytes must bigger then 0, but get %d!", totalHoldSize));
        }
        this.totalHoldSize = totalHoldSize;
    }

    private MessageAccumulation getOrCreateSyncSendBatch(AggregateKey aggregateKey,
        DefaultMQProducer defaultMQProducer) {
        MessageAccumulation batch = syncSendBatchs.get(aggregateKey);
        if (batch != null) {
            return batch;
        }
        batch = new MessageAccumulation(aggregateKey, defaultMQProducer);
        MessageAccumulation previous = syncSendBatchs.putIfAbsent(aggregateKey, batch);

        return previous == null ? batch : previous;
    }

    private MessageAccumulation getOrCreateAsyncSendBatch(AggregateKey aggregateKey,
        DefaultMQProducer defaultMQProducer) {
        MessageAccumulation batch = asyncSendBatchs.get(aggregateKey);
        if (batch != null) {
            return batch;
        }
        batch = new MessageAccumulation(aggregateKey, defaultMQProducer);
        MessageAccumulation previous = asyncSendBatchs.putIfAbsent(aggregateKey, batch);

        return previous == null ? batch : previous;
    }

    SendResult send(Message msg,
        DefaultMQProducer defaultMQProducer) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        AggregateKey partitionKey = new AggregateKey(msg);
        while (true) {
            MessageAccumulation batch = getOrCreateSyncSendBatch(partitionKey, defaultMQProducer);
            int index = batch.add(msg);
            if (index == -1) {
                syncSendBatchs.remove(partitionKey, batch);
            } else {
                return batch.sendResults[index];
            }
        }
    }

    SendResult send(Message msg, MessageQueue mq,
        DefaultMQProducer defaultMQProducer) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        AggregateKey partitionKey = new AggregateKey(msg, mq);
        while (true) {
            MessageAccumulation batch = getOrCreateSyncSendBatch(partitionKey, defaultMQProducer);
            int index = batch.add(msg);
            if (index == -1) {
                syncSendBatchs.remove(partitionKey, batch);
            } else {
                return batch.sendResults[index];
            }
        }
    }

    void send(Message msg, SendCallback sendCallback,
        DefaultMQProducer defaultMQProducer) throws InterruptedException, RemotingException, MQClientException {
        AggregateKey partitionKey = new AggregateKey(msg);
        while (true) {
            MessageAccumulation batch = getOrCreateAsyncSendBatch(partitionKey, defaultMQProducer);
            if (!batch.add(msg, sendCallback)) {
                asyncSendBatchs.remove(partitionKey, batch);
            } else {
                return;
            }
        }
    }

    void send(Message msg, MessageQueue mq,
        SendCallback sendCallback,
        DefaultMQProducer defaultMQProducer) throws InterruptedException, RemotingException, MQClientException {
        AggregateKey partitionKey = new AggregateKey(msg, mq);
        while (true) {
            MessageAccumulation batch = getOrCreateAsyncSendBatch(partitionKey, defaultMQProducer);
            if (!batch.add(msg, sendCallback)) {
                asyncSendBatchs.remove(partitionKey, batch);
            } else {
                return;
            }
        }
    }

    boolean tryAddMessage(Message message) {
        synchronized (currentlyHoldSize) {
            if (currentlyHoldSize.get() < totalHoldSize) {
                int bodySize = null == message.getBody() ? 0 : message.getBody().length;
                if (bodySize > 0) {
                    currentlyHoldSize.addAndGet(bodySize);
                }
                return true;
            } else {
                return false;
            }
        }
    }

    private class AggregateKey {
        /**
         * 消息主题名称
         * 作用：标识消息所属的主题，相同主题的消息可以聚合
         */
        public String topic = null;

        /**
         * 消息队列
         * 作用：指定消息发送的目标队列，相同队列的消息可以聚合
         * 为null时表示由负载均衡算法选择队列
         */
        public MessageQueue mq = null;

        /**
         * 是否等待存储确认
         * 作用：控制消息的可靠性级别，相同级别的消息可以聚合
         * true: 等待消息落盘后返回，false: 消息到达内存即返回
         */
        public boolean waitStoreMsgOK = false;

        /**
         * 消息标签
         * 作用：消息的业务标识，相同标签的消息可以聚合
         * 用于消息过滤和分类
         */
        public String tag = null;

        public AggregateKey(Message message) {
            this(message.getTopic(), null, message.isWaitStoreMsgOK(), message.getTags());
        }

        public AggregateKey(Message message, MessageQueue mq) {
            this(message.getTopic(), mq, message.isWaitStoreMsgOK(), message.getTags());
        }

        public AggregateKey(String topic, MessageQueue mq, boolean waitStoreMsgOK, String tag) {
            this.topic = topic;
            this.mq = mq;
            this.waitStoreMsgOK = waitStoreMsgOK;
            this.tag = tag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AggregateKey key = (AggregateKey) o;
            return waitStoreMsgOK == key.waitStoreMsgOK && topic.equals(key.topic) && Objects.equals(mq, key.mq) && Objects.equals(tag, key.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, mq, waitStoreMsgOK, tag);
        }
    }

    private class MessageAccumulation {
        /**
         * 默认MQ生产者实例
         * 作用：用于执行实际的消息发送操作
         */
        private final DefaultMQProducer defaultMQProducer;

        /**
         * 累积的消息列表
         * 作用：存储待批量发送的消息，按添加顺序排列
         */
        private LinkedList<Message> messages;

        /**
         * 异步发送回调列表
         * 作用：存储每个消息对应的异步回调，与messages一一对应
         * 仅在异步发送时使用
         */
        private LinkedList<SendCallback> sendCallbacks;

        /**
         * 消息键集合
         * 作用：收集所有消息的keys，用于构建批量消息的keys属性
         * 便于消息检索和去重
         */
        private Set<String> keys;

        /**
         * 批次关闭状态标识
         * 作用：标记当前批次是否已关闭，防止重复发送
         * 线程安全：使用AtomicBoolean保证并发访问的安全性
         */
        private final AtomicBoolean closed;

        /**
         * 发送结果数组
         * 作用：存储批量发送后拆分的每个消息的发送结果
         * 数组索引与messages中的消息顺序对应
         */
        private SendResult[] sendResults;

        /**
         * 聚合键
         * 作用：标识当前批次的聚合条件，相同聚合键的消息会被聚合到同一批次
         */
        private AggregateKey aggregateKey;

        /**
         * 累积消息的总字节数
         * 作用：跟踪当前批次中所有消息体的总大小
         * 用于判断是否达到批量发送的大小阈值
         */
        private AtomicInteger messagesSize;

        /**
         * 消息计数
         * 作用：记录当前批次中的消息数量
         * 用于数组索引和结果拆分
         */
        private int count;

        /**
         * 批次创建时间戳
         * 作用：记录批次创建的时间，用于判断是否达到时间阈值
         * 与holdMs配合实现基于时间的批量发送触发
         */
        private long createTime;

        public MessageAccumulation(AggregateKey aggregateKey, DefaultMQProducer defaultMQProducer) {
            this.defaultMQProducer = defaultMQProducer;
            this.messages = new LinkedList<Message>();
            this.sendCallbacks = new LinkedList<SendCallback>();
            this.keys = new HashSet<String>();
            this.closed = new AtomicBoolean(false);
            this.messagesSize = new AtomicInteger(0);
            this.aggregateKey = aggregateKey;
            this.count = 0;
            this.createTime = System.currentTimeMillis();
        }

        private boolean readyToSend() {
            if (this.messagesSize.get() > holdSize
                || System.currentTimeMillis() >= this.createTime + holdMs) {
                return true;
            }
            return false;
        }

        public int add(Message msg) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
            int ret = -1;
            synchronized (this.closed) {
                if (this.closed.get()) {
                    return ret;
                }
                ret = this.count++;
                this.messages.add(msg);
                int bodySize = null == msg.getBody() ? 0 : msg.getBody().length;
                if (bodySize > 0) {
                    messagesSize.addAndGet(bodySize);
                }
                String msgKeys = msg.getKeys();
                if (msgKeys != null) {
                    this.keys.addAll(Arrays.asList(msgKeys.split(MessageConst.KEY_SEPARATOR)));
                }
            }
            synchronized (this) {
                while (!this.closed.get()) {
                    if (readyToSend()) {
                        this.send();
                        break;
                    } else {
                        this.wait();
                    }
                }
                return ret;
            }
        }

        public boolean add(Message msg,
            SendCallback sendCallback) throws InterruptedException, RemotingException, MQClientException {
            synchronized (this.closed) {
                if (this.closed.get()) {
                    return false;
                }
                this.count++;
                this.messages.add(msg);
                this.sendCallbacks.add(sendCallback);
                int bodySize = null == msg.getBody() ? 0 : msg.getBody().length;
                if (bodySize > 0) {
                    messagesSize.addAndGet(bodySize);
                }
            }
            if (readyToSend()) {
                this.send(sendCallback);
            }
            return true;

        }

        public synchronized void wakeup() {
            if (this.closed.get()) {
                return;
            }
            this.notify();
        }

        private MessageBatch batch() {
            MessageBatch messageBatch = new MessageBatch(this.messages);
            messageBatch.setTopic(this.aggregateKey.topic);
            messageBatch.setWaitStoreMsgOK(this.aggregateKey.waitStoreMsgOK);
            messageBatch.setKeys(this.keys);
            messageBatch.setTags(this.aggregateKey.tag);
            MessageClientIDSetter.setUniqID(messageBatch);
            messageBatch.setBody(MessageDecoder.encodeMessages(this.messages));
            return messageBatch;
        }

        private void splitSendResults(SendResult sendResult) {
            if (sendResult == null) {
                throw new IllegalArgumentException("sendResult is null");
            }
            boolean isBatchConsumerQueue = !sendResult.getMsgId().contains(",");
            this.sendResults = new SendResult[this.count];
            if (!isBatchConsumerQueue) {
                String[] msgIds = sendResult.getMsgId().split(",");
                String[] offsetMsgIds = sendResult.getOffsetMsgId().split(",");
                if (offsetMsgIds.length != this.count || msgIds.length != this.count) {
                    throw new IllegalArgumentException("sendResult is illegal");
                }
                for (int i = 0; i < this.count; i++) {
                    this.sendResults[i] = new SendResult(sendResult.getSendStatus(), msgIds[i],
                        sendResult.getMessageQueue(), sendResult.getQueueOffset() + i,
                        sendResult.getTransactionId(), offsetMsgIds[i], sendResult.getRegionId());
                }
            } else {
                for (int i = 0; i < this.count; i++) {
                    this.sendResults[i] = sendResult;
                }
            }
        }

        private void send() throws InterruptedException, MQClientException, MQBrokerException, RemotingException {
            synchronized (this.closed) {
                if (this.closed.getAndSet(true)) {
                    return;
                }
            }
            MessageBatch messageBatch = this.batch();
            SendResult sendResult = null;
            try {
                if (defaultMQProducer != null) {
                    sendResult = defaultMQProducer.sendDirect(messageBatch, aggregateKey.mq, null);
                    this.splitSendResults(sendResult);
                } else {
                    throw new IllegalArgumentException("defaultMQProducer is null, can not send message");
                }
            } finally {
                currentlyHoldSize.addAndGet(-messagesSize.get());
                this.notifyAll();
            }
        }

        private void send(SendCallback sendCallback) {
            synchronized (this.closed) {
                if (this.closed.getAndSet(true)) {
                    return;
                }
            }
            MessageBatch messageBatch = this.batch();
            SendResult sendResult = null;
            try {
                if (defaultMQProducer != null) {
                    final int size = messagesSize.get();
                    defaultMQProducer.sendDirect(messageBatch, aggregateKey.mq, new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            try {
                                splitSendResults(sendResult);
                                int i = 0;
                                Iterator<SendCallback> it = sendCallbacks.iterator();
                                while (it.hasNext()) {
                                    SendCallback v = it.next();
                                    v.onSuccess(sendResults[i++]);
                                }
                                if (i != count) {
                                    throw new IllegalArgumentException("sendResult is illegal");
                                }
                                currentlyHoldSize.addAndGet(-size);
                            } catch (Exception e) {
                                onException(e);
                            }
                        }

                        @Override
                        public void onException(Throwable e) {
                            for (SendCallback v : sendCallbacks) {
                                v.onException(e);
                            }
                            currentlyHoldSize.addAndGet(-size);
                        }
                    });
                } else {
                    throw new IllegalArgumentException("defaultMQProducer is null, can not send message");
                }
            } catch (Exception e) {
                for (SendCallback v : sendCallbacks) {
                    v.onException(e);
                }
            }
        }
    }
}
