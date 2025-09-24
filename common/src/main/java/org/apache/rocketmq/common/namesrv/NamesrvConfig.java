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

/**
 * $Id: NamesrvConfig.java 1839 2013-05-16 02:12:02Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.namesrv;

import java.io.File;
import org.apache.rocketmq.common.MixAll;

/**
 * RocketMQ NameServer 配置类
 *
 * <p>作用和职责：</p>
 * <ul>
 *   <li><b>配置管理</b>：管理NameServer运行时的各种配置参数</li>
 *   <li><b>性能调优</b>：提供线程池、队列容量等性能相关配置</li>
 *   <li><b>功能开关</b>：控制NameServer的各种功能特性开关</li>
 *   <li><b>路径配置</b>：管理配置文件、数据文件的存储路径</li>
 *   <li><b>集群配置</b>：支持集群模式和高可用相关配置</li>
 * </ul>
 *
 * <p>主要配置分类：</p>
 * <ul>
 *   <li><b>基础配置</b>：RocketMQ安装目录、环境名称等</li>
 *   <li><b>存储配置</b>：配置文件路径、KV存储路径等</li>
 *   <li><b>线程池配置</b>：处理客户端和Broker请求的线程池参数</li>
 *   <li><b>功能开关</b>：顺序消息、Topic列表、Controller等功能开关</li>
 *   <li><b>高可用配置</b>：主从切换、故障检测等高可用特性</li>
 * </ul>
 *
 * <p>配置文件示例：</p>
 * <pre>
 * # NameServer基础配置
 * rocketmqHome=/opt/rocketmq
 * productEnvName=production
 *
 * # 线程池配置
 * clientRequestThreadPoolNums=16
 * defaultThreadPoolNums=32
 *
 * # 功能开关
 * orderMessageEnable=true
 * enableControllerInNamesrv=true
 * </pre>
 *
 * @author RocketMQ Team
 * @since 3.0.0
 */
public class NamesrvConfig {

    /**
     * RocketMQ安装根目录
     * <p>用于定位RocketMQ的安装路径，影响日志、配置文件等的默认位置</p>
     * <p>默认值：从系统属性ROCKETMQ_HOME获取</p>
     */
    private String rocketmqHome = MixAll.ROCKETMQ_HOME_DIR;

    /**
     * KV配置存储路径
     * <p>用于存储NameServer的键值对配置信息，如Topic配置、Broker信息等</p>
     * <p>默认值：${user.home}/namesrv/kvConfig.json</p>
     */
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";

    /**
     * NameServer配置文件存储路径
     * <p>用于持久化NameServer的配置信息</p>
     * <p>默认值：${user.home}/namesrv/namesrv.properties</p>
     */
    private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";

    /**
     * 产品环境名称
     * <p>用于标识当前运行环境，可用于环境隔离和配置区分</p>
     * <p>默认值：center</p>
     */
    private String productEnvName = "center";

    /**
     * 集群测试模式开关
     * <p>是否启用集群测试模式，测试模式下可能会有不同的行为</p>
     * <p>默认值：false</p>
     */
    private boolean clusterTest = false;

    /**
     * 顺序消息功能开关
     * <p>是否启用顺序消息功能，影响Topic路由和消息队列的处理</p>
     * <p>默认值：false</p>
     */
    private boolean orderMessageEnable = false;

    /**
     * 是否向Broker返回顺序Topic配置
     * <p>控制NameServer是否将顺序Topic的配置信息返回给Broker</p>
     * <p>默认值：true</p>
     */
    private boolean returnOrderTopicConfigToBroker = true;

    /**
     * 客户端请求处理线程池大小
     * <p>处理客户端请求（如GET_ROUTEINTO_BY_TOPIC）的线程数量</p>
     * <p>默认值：8</p>
     */
    private int clientRequestThreadPoolNums = 8;

    /**
     * 默认请求处理线程池大小
     * <p>处理Broker注册等操作请求（如REGISTER_BROKER）的线程数量</p>
     * <p>默认值：16</p>
     */
    private int defaultThreadPoolNums = 16;

    /**
     * 客户端请求队列容量
     * <p>客户端请求处理队列的最大容量，超过此容量的请求将被拒绝</p>
     * <p>默认值：50000</p>
     */
    private int clientRequestThreadPoolQueueCapacity = 50000;

    /**
     * 默认请求队列容量
     * <p>Broker和操作请求处理队列的最大容量</p>
     * <p>默认值：10000</p>
     */
    private int defaultThreadPoolQueueCapacity = 10000;

    /**
     * 扫描非活跃Broker的时间间隔（毫秒）
     * <p>NameServer定期扫描并清理非活跃Broker的时间间隔</p>
     * <p>默认值：5000毫秒（5秒）</p>
     */
    private long scanNotActiveBrokerInterval = 5 * 1000;

    /**
     * Broker注销队列容量
     * <p>处理Broker注销请求的队列容量</p>
     * <p>默认值：3000</p>
     */
    private int unRegisterBrokerQueueCapacity = 3000;

    /**
     * 是否支持代理Master功能
     * <p>当Master节点宕机时，Slave是否可以作为代理Master提供以下服务：</p>
     * <ul>
     *   <li>支持消息队列的锁定/解锁操作</li>
     *   <li>支持searchOffset、查询maxOffset/minOffset操作</li>
     *   <li>支持查询最早消息存储时间</li>
     * </ul>
     * <p>默认值：false</p>
     */
    private boolean supportActingMaster = false;

    /**
     * 是否启用所有Topic列表功能
     * <p>控制是否允许获取所有Topic的列表信息</p>
     * <p>默认值：true</p>
     */
    private volatile boolean enableAllTopicList = true;

    /**
     * 是否启用Topic列表功能
     * <p>控制是否允许获取Topic列表信息</p>
     * <p>默认值：true</p>
     */
    private volatile boolean enableTopicList = true;

    /**
     * 是否通知最小BrokerId变更
     * <p>当集群中最小BrokerId发生变化时是否发送通知</p>
     * <p>默认值：false</p>
     */
    private volatile boolean notifyMinBrokerIdChanged = false;

    /**
     * 是否在NameServer中启用Controller
     * <p>是否在当前NameServer实例中启动Controller管理器</p>
     * <p>Controller用于管理Broker的主从切换和故障转移</p>
     * <p>默认值：false</p>
     */
    private boolean enableControllerInNamesrv = false;

    /**
     * 是否需要等待服务就绪
     * <p>启动时是否需要等待相关服务完全就绪后再对外提供服务</p>
     * <p>默认值：false</p>
     */
    private volatile boolean needWaitForService = false;

    /**
     * 等待服务就绪的时间（秒）
     * <p>当needWaitForService为true时，等待服务就绪的最大时间</p>
     * <p>默认值：45秒</p>
     */
    private int waitSecondsForService = 45;

    /**
     * 是否随Broker注册删除Topic
     * <p>启用此标志后，Broker注册时不包含的Topic将从NameServer中删除</p>
     *
     * <p><b>警告：</b></p>
     * <ul>
     *   <li>启用此标志时，需同时启用Broker配置中的"enableSingleTopicRegister"以避免意外丢失Topic路由信息</li>
     *   <li>此标志目前不支持静态Topic</li>
     * </ul>
     * <p>默认值：false</p>
     */
    private boolean deleteTopicWithBrokerRegistration = false;

    /**
     * 配置黑名单
     * <p>此黑名单中的配置项不允许通过命令进行更新，只能通过重启进程来更新</p>
     * <p>多个配置项用分号分隔</p>
     * <p>默认值：configBlackList;configStorePath;kvConfigPath</p>
     */
    private String configBlackList = "configBlackList;configStorePath;kvConfigPath";

    public String getConfigBlackList() {
        return configBlackList;
    }

    public void setConfigBlackList(String configBlackList) {
        this.configBlackList = configBlackList;
    }

    public boolean isOrderMessageEnable() {
        return orderMessageEnable;
    }

    public void setOrderMessageEnable(boolean orderMessageEnable) {
        this.orderMessageEnable = orderMessageEnable;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getKvConfigPath() {
        return kvConfigPath;
    }

    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }

    public String getProductEnvName() {
        return productEnvName;
    }

    public void setProductEnvName(String productEnvName) {
        this.productEnvName = productEnvName;
    }

    public boolean isClusterTest() {
        return clusterTest;
    }

    public void setClusterTest(boolean clusterTest) {
        this.clusterTest = clusterTest;
    }

    public String getConfigStorePath() {
        return configStorePath;
    }

    public void setConfigStorePath(final String configStorePath) {
        this.configStorePath = configStorePath;
    }

    public boolean isReturnOrderTopicConfigToBroker() {
        return returnOrderTopicConfigToBroker;
    }

    public void setReturnOrderTopicConfigToBroker(boolean returnOrderTopicConfigToBroker) {
        this.returnOrderTopicConfigToBroker = returnOrderTopicConfigToBroker;
    }

    public int getClientRequestThreadPoolNums() {
        return clientRequestThreadPoolNums;
    }

    public void setClientRequestThreadPoolNums(final int clientRequestThreadPoolNums) {
        this.clientRequestThreadPoolNums = clientRequestThreadPoolNums;
    }

    public int getDefaultThreadPoolNums() {
        return defaultThreadPoolNums;
    }

    public void setDefaultThreadPoolNums(final int defaultThreadPoolNums) {
        this.defaultThreadPoolNums = defaultThreadPoolNums;
    }

    public int getClientRequestThreadPoolQueueCapacity() {
        return clientRequestThreadPoolQueueCapacity;
    }

    public void setClientRequestThreadPoolQueueCapacity(final int clientRequestThreadPoolQueueCapacity) {
        this.clientRequestThreadPoolQueueCapacity = clientRequestThreadPoolQueueCapacity;
    }

    public int getDefaultThreadPoolQueueCapacity() {
        return defaultThreadPoolQueueCapacity;
    }

    public void setDefaultThreadPoolQueueCapacity(final int defaultThreadPoolQueueCapacity) {
        this.defaultThreadPoolQueueCapacity = defaultThreadPoolQueueCapacity;
    }

    public long getScanNotActiveBrokerInterval() {
        return scanNotActiveBrokerInterval;
    }

    public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
        this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
    }

    public int getUnRegisterBrokerQueueCapacity() {
        return unRegisterBrokerQueueCapacity;
    }

    public void setUnRegisterBrokerQueueCapacity(final int unRegisterBrokerQueueCapacity) {
        this.unRegisterBrokerQueueCapacity = unRegisterBrokerQueueCapacity;
    }

    public boolean isSupportActingMaster() {
        return supportActingMaster;
    }

    public void setSupportActingMaster(final boolean supportActingMaster) {
        this.supportActingMaster = supportActingMaster;
    }

    public boolean isEnableAllTopicList() {
        return enableAllTopicList;
    }

    public void setEnableAllTopicList(boolean enableAllTopicList) {
        this.enableAllTopicList = enableAllTopicList;
    }

    public boolean isEnableTopicList() {
        return enableTopicList;
    }

    public void setEnableTopicList(boolean enableTopicList) {
        this.enableTopicList = enableTopicList;
    }

    public boolean isNotifyMinBrokerIdChanged() {
        return notifyMinBrokerIdChanged;
    }

    public void setNotifyMinBrokerIdChanged(boolean notifyMinBrokerIdChanged) {
        this.notifyMinBrokerIdChanged = notifyMinBrokerIdChanged;
    }

    public boolean isEnableControllerInNamesrv() {
        return enableControllerInNamesrv;
    }

    public void setEnableControllerInNamesrv(boolean enableControllerInNamesrv) {
        this.enableControllerInNamesrv = enableControllerInNamesrv;
    }

    public boolean isNeedWaitForService() {
        return needWaitForService;
    }

    public void setNeedWaitForService(boolean needWaitForService) {
        this.needWaitForService = needWaitForService;
    }

    public int getWaitSecondsForService() {
        return waitSecondsForService;
    }

    public void setWaitSecondsForService(int waitSecondsForService) {
        this.waitSecondsForService = waitSecondsForService;
    }

    public boolean isDeleteTopicWithBrokerRegistration() {
        return deleteTopicWithBrokerRegistration;
    }

    public void setDeleteTopicWithBrokerRegistration(boolean deleteTopicWithBrokerRegistration) {
        this.deleteTopicWithBrokerRegistration = deleteTopicWithBrokerRegistration;
    }
}
