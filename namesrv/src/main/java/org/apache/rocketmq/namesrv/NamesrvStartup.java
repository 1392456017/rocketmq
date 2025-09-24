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
package org.apache.rocketmq.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.JraftConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * RocketMQ NameServer 启动类
 *
 * <p>作用和职责：</p>
 * <ul>
 *   <li><b>NameServer启动入口</b>：提供NameServer服务的主启动入口</li>
 *   <li><b>配置管理</b>：解析命令行参数和配置文件，初始化各种配置对象</li>
 *   <li><b>组件创建</b>：创建和初始化NamesrvController和ControllerManager</li>
 *   <li><b>生命周期管理</b>：管理NameServer的启动、运行和关闭流程</li>
 *   <li><b>优雅关闭</b>：注册JVM关闭钩子，确保服务优雅停止</li>
 * </ul>
 *
 * <p>NameServer在RocketMQ中的作用：</p>
 * <ul>
 *   <li><b>路由注册中心</b>：管理Broker的路由信息，包括Topic和队列的分布</li>
 *   <li><b>服务发现</b>：为生产者和消费者提供Broker地址查询服务</li>
 *   <li><b>元数据管理</b>：存储和管理Topic配置、Broker状态等元数据</li>
 *   <li><b>负载均衡支持</b>：为客户端提供Broker选择的基础数据</li>
 * </ul>
 *
 * <p>启动流程：</p>
 * <ol>
 *   <li>解析命令行参数和配置文件</li>
 *   <li>创建NamesrvController实例</li>
 *   <li>初始化并启动NameServer服务</li>
 *   <li>可选：启动ControllerManager（用于主从切换管理）</li>
 *   <li>注册关闭钩子，等待服务运行</li>
 * </ol>
 *
 * @author RocketMQ Team
 * @since 3.0.0
 */
public class NamesrvStartup {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    /** 控制台日志记录器，用于输出配置信息 */
    private static final Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);

    /** 配置文件属性集合 */
    private static Properties properties = null;

    /** NameServer 配置对象 */
    private static NamesrvConfig namesrvConfig = null;

    /** Netty 服务端配置对象 */
    private static NettyServerConfig nettyServerConfig = null;

    /** Netty 客户端配置对象 */
    private static NettyClientConfig nettyClientConfig = null;

    /** 控制器配置对象（用于主从切换管理） */
    private static ControllerConfig controllerConfig = null;

    /**
     * NameServer 主启动方法
     *
     * <p>程序入口点，负责启动NameServer和可选的ControllerManager</p>
     *
     * @param args 命令行参数
     *             -c configFile: 指定配置文件路径
     *             -p: 打印所有配置项并退出
     */
    public static void main(String[] args) {
        // 启动 NameServer 核心服务
        main0(args);
        // 启动 Controller 管理器（如果启用）
        controllerManagerMain();
    }

    /**
     * NameServer 核心启动方法
     *
     * <p>解析配置并创建启动NameServer控制器</p>
     *
     * @param args 命令行参数
     * @return NamesrvController 实例，如果启动失败则返回null
     */
    public static NamesrvController main0(String[] args) {
        try {
            // 1. 解析命令行参数和配置文件
            parseCommandlineAndConfigFile(args);
            // 2. 创建并启动 NameServer 控制器
            NamesrvController controller = createAndStartNamesrvController();
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * ControllerManager 启动方法
     *
     * <p>如果启用了Controller功能，则创建并启动ControllerManager</p>
     * <p>ControllerManager用于管理Broker的主从切换和故障转移</p>
     *
     * @return ControllerManager 实例，如果未启用或启动失败则返回null
     */
    public static ControllerManager controllerManagerMain() {
        try {
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                return createAndStartControllerManager();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    /**
     * 解析命令行参数和配置文件
     *
     * <p>功能包括：</p>
     * <ul>
     *   <li>设置RocketMQ版本信息</li>
     *   <li>解析命令行选项（-c 配置文件路径，-p 打印配置）</li>
     *   <li>加载配置文件并映射到配置对象</li>
     *   <li>初始化各种配置对象（NamesrvConfig、NettyServerConfig等）</li>
     *   <li>验证必要的环境变量（ROCKETMQ_HOME）</li>
     * </ul>
     *
     * @param args 命令行参数数组
     * @throws Exception 配置解析或文件读取异常
     */
    public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
        // 设置 RocketMQ 版本信息到系统属性
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        // 构建命令行选项
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
            return;
        }

        // 初始化配置对象
        namesrvConfig = new NamesrvConfig();
        nettyServerConfig = new NettyServerConfig();
        nettyClientConfig = new NettyClientConfig();
        // 设置 NameServer 默认监听端口为 9876
        nettyServerConfig.setListenPort(9876);

        // 处理配置文件选项 (-c)
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                // 读取配置文件
                InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
                properties = new Properties();
                properties.load(in);

                // 将配置文件属性映射到配置对象
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                MixAll.properties2Object(properties, nettyClientConfig);

                // 如果启用了 Controller 功能，初始化相关配置
                if (namesrvConfig.isEnableControllerInNamesrv()) {
                    controllerConfig = new ControllerConfig();
                    JraftConfig jraftConfig = new JraftConfig();
                    controllerConfig.setJraftConfig(jraftConfig);
                    MixAll.properties2Object(properties, controllerConfig);
                    MixAll.properties2Object(properties, jraftConfig);
                }

                namesrvConfig.setConfigStorePath(file);
                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        // 将命令行参数映射到配置对象
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        // 处理打印配置选项 (-p)
        if (commandLine.hasOption('p')) {
            MixAll.printObjectProperties(logConsole, namesrvConfig);
            MixAll.printObjectProperties(logConsole, nettyServerConfig);
            MixAll.printObjectProperties(logConsole, nettyClientConfig);
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                MixAll.printObjectProperties(logConsole, controllerConfig);
            }
            System.exit(0);
        }

        // 验证 ROCKETMQ_HOME 环境变量
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // 打印最终配置信息到日志
        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
    }

    /**
     * 创建并启动 NameServer 控制器
     *
     * <p>完整的创建和启动流程，包括成功提示信息输出</p>
     *
     * @return 启动成功的 NamesrvController 实例
     * @throws Exception 创建或启动过程中的异常
     */
    public static NamesrvController createAndStartNamesrvController() throws Exception {
        // 创建 NameServer 控制器
        NamesrvController controller = createNamesrvController();
        // 启动控制器
        start(controller);

        // 输出启动成功信息
        NettyServerConfig serverConfig = controller.getNettyServerConfig();
        String tip = String.format("The Name Server boot success. serializeType=%s, address %s:%d",
            RemotingCommand.getSerializeTypeConfigInThisServer(),
            serverConfig.getBindAddress(),
            serverConfig.getListenPort());
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controller;
    }

    /**
     * 创建 NameServer 控制器实例
     *
     * <p>使用解析好的配置对象创建NamesrvController，并注册配置信息</p>
     *
     * @return 新创建的 NamesrvController 实例
     */
    public static NamesrvController createNamesrvController() {
        // 创建 NameServer 控制器，传入各种配置对象
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);
        // 注册配置属性，防止配置丢失
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    /**
     * 启动 NameServer 控制器
     *
     * <p>启动流程：</p>
     * <ol>
     *   <li>初始化控制器</li>
     *   <li>注册JVM关闭钩子</li>
     *   <li>启动服务</li>
     * </ol>
     *
     * @param controller 要启动的 NamesrvController 实例
     * @return 启动后的控制器实例
     * @throws Exception 初始化或启动异常
     */
    public static NamesrvController start(final NamesrvController controller) throws Exception {
        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        // 初始化控制器
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        // 注册JVM关闭钩子，确保优雅关闭
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        // 启动控制器服务
        controller.start();

        return controller;
    }

    /**
     * 创建并启动 Controller 管理器
     *
     * <p>ControllerManager用于管理Broker的主从切换和故障转移</p>
     *
     * @return 启动成功的 ControllerManager 实例
     * @throws Exception 创建或启动过程中的异常
     */
    public static ControllerManager createAndStartControllerManager() throws Exception {
        ControllerManager controllerManager = createControllerManager();
        start(controllerManager);

        String tip = "The ControllerManager boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controllerManager;
    }

    /**
     * 创建 Controller 管理器实例
     *
     * @return 新创建的 ControllerManager 实例
     * @throws Exception 创建过程中的异常
     */
    public static ControllerManager createControllerManager() throws Exception {
        // 克隆网络服务配置，避免配置冲突
        NettyServerConfig controllerNettyServerConfig = (NettyServerConfig) nettyServerConfig.clone();
        ControllerManager controllerManager = new ControllerManager(controllerConfig, controllerNettyServerConfig, nettyClientConfig);
        // 注册配置属性
        controllerManager.getConfiguration().registerConfig(properties);
        return controllerManager;
    }

    /**
     * 启动 Controller 管理器
     *
     * @param controllerManager 要启动的 ControllerManager 实例
     * @return 启动后的管理器实例
     * @throws Exception 初始化或启动异常
     */
    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {
        if (null == controllerManager) {
            throw new IllegalArgumentException("ControllerManager is null");
        }

        // 初始化管理器
        boolean initResult = controllerManager.initialize();
        if (!initResult) {
            controllerManager.shutdown();
            System.exit(-3);
        }

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controllerManager.shutdown();
            return null;
        }));

        // 启动管理器服务
        controllerManager.start();

        return controllerManager;
    }

    /**
     * 关闭 NameServer 控制器
     *
     * @param controller 要关闭的控制器实例
     */
    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    /**
     * 关闭 Controller 管理器
     *
     * @param controllerManager 要关闭的管理器实例
     */
    public static void shutdown(final ControllerManager controllerManager) {
        controllerManager.shutdown();
    }

    /**
     * 构建命令行选项
     *
     * <p>支持的选项：</p>
     * <ul>
     *   <li>-c configFile: 指定配置文件路径</li>
     *   <li>-p: 打印所有配置项并退出</li>
     * </ul>
     *
     * @param options 基础选项对象
     * @return 添加了NameServer特定选项的Options对象
     */
    public static Options buildCommandlineOptions(final Options options) {
        // 配置文件选项
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        // 打印配置选项
        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    /**
     * 获取配置属性集合
     *
     * @return 从配置文件加载的属性集合
     */
    public static Properties getProperties() {
        return properties;
    }
}
