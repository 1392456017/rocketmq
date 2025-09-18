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
package org.apache.rocketmq.client.impl.producer;

import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

import java.util.Set;
// MQProducerInner 是一个内部生产者接口，定义了生产者内部需要实现的核心功能。
public interface MQProducerInner {
    // 获取发布主题列表
    Set<String> getPublishTopicList();
    // 检查主题是否需要更新
    boolean isPublishTopicNeedUpdate(final String topic);

    TransactionCheckListener checkListener();
    // 获取事务监听器
    TransactionListener getCheckListener();
    // 检查事务状态
    void checkTransactionState(
        final String addr,
        final MessageExt msg,
        final CheckTransactionStateRequestHeader checkRequestHeader);
    // 更新主题发布信息
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);
    // 是否单元模式
    boolean isUnitMode();
}
