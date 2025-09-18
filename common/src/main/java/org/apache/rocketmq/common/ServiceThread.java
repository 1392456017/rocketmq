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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * RocketMQ 服务线程基类
 *
 * 作用：为 RocketMQ 中的各种后台服务线程提供统一的生命周期管理和线程控制机制
 *
 * 主要功能：
 * 1. 线程生命周期管理：启动、停止、重启
 * 2. 线程间通信：唤醒、等待机制
 * 3. 优雅关闭：支持中断和超时等待
 * 4. 状态管理：线程运行状态的跟踪和控制
 *
 * 使用场景：
 * - 消息拉取服务 (PullMessageService)
 * - 负载均衡服务 (RebalanceService)
 * - 定时任务服务 (各种定时清理、检查任务)
 * - 网络通信服务 (Netty 相关服务线程)
 */
public abstract class ServiceThread implements Runnable {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 线程关闭时的最大等待时间：90秒
     * 作用：防止线程关闭时无限等待，确保应用能够正常退出
     */
    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 工作线程实例
     * 作用：执行具体的服务逻辑，由子类实现 run() 方法定义行为
     */
    protected Thread thread;

    /**
     * 线程等待点，基于 CountDownLatch 实现
     * 作用：实现线程的等待和唤醒机制，支持超时等待
     * 使用场景：当没有工作时让线程进入等待状态，有工作时唤醒线程
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    /**
     * 通知状态标识
     * 作用：标记是否已经发送了唤醒通知，防止重复通知
     * 线程安全：使用 AtomicBoolean 保证并发环境下的状态一致性
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 停止状态标识
     * 作用：标记线程是否应该停止运行，子类在 run() 方法中检查此标识
     * volatile：保证多线程环境下状态变更的可见性
     */
    protected volatile boolean stopped = false;

    /**
     * 守护线程标识
     * 作用：控制线程是否为守护线程
     * true: 守护线程，JVM 退出时不等待此线程结束
     * false: 用户线程，JVM 退出时会等待此线程结束
     */
    protected boolean isDaemon = false;

    /**
     * 启动状态标识
     * 作用：防止重复启动同一个服务线程，支持线程的重启功能
     * 线程安全：使用 AtomicBoolean 实现 CAS 操作，确保启动的原子性
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    /**
     * 获取服务名称
     * 作用：为线程提供有意义的名称，便于调试和监控
     * 子类必须实现此方法，返回具体的服务名称
     */
    public abstract String getServiceName();

    /**
     * 启动服务线程
     * 作用：创建并启动工作线程，支持重复调用（幂等操作）
     *
     * 实现机制：
     * 1. 使用 CAS 操作防止重复启动
     * 2. 重置停止状态，支持重启
     * 3. 创建新线程并设置守护线程属性
     * 4. 启动线程开始执行 run() 方法
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
        log.info("Start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
    }

    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 优雅关闭服务线程
     * 作用：安全地停止线程运行，支持中断和超时等待
     *
     * @param interrupt 是否使用中断方式强制停止线程
     *
     * 实现机制：
     * 1. 使用 CAS 操作防止重复关闭
     * 2. 设置停止标识，通知线程应该退出
     * 3. 唤醒可能正在等待的线程
     * 4. 可选择性地中断线程
     * 5. 等待线程结束，最多等待 JOIN_TIME 时间
     * 6. 记录关闭耗时，便于性能监控
     */
    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread[{}] interrupt={} ", getServiceName(), interrupt);

        // 如果线程正在等待，唤醒它以便检查停止状态
        wakeup();

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJoinTime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread[{}], elapsed time: {}ms, join time:{}ms", getServiceName(), elapsedTime, this.getJoinTime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJoinTime() {
        return JOIN_TIME;
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread[{}] ", this.getServiceName());
    }

    /**
     * 唤醒等待中的线程
     * 作用：通知线程有新的工作需要处理，或者需要检查停止状态
     *
     * 实现机制：
     * 1. 使用 CAS 操作防止重复唤醒
     * 2. 通过 CountDownLatch 机制唤醒等待的线程
     * 3. 避免无效的唤醒操作，提高性能
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 等待指定时间或直到被唤醒
     * 作用：让线程在没有工作时进入等待状态，节省 CPU 资源
     *
     * @param interval 最大等待时间（毫秒）
     *
     * 实现机制：
     * 1. 检查是否已有唤醒通知，如有则立即返回
     * 2. 重置等待点，准备进入等待状态
     * 3. 等待指定时间或直到被唤醒
     * 4. 清理通知状态，调用等待结束回调
     *
     * 使用场景：
     * - 消息拉取服务等待新的拉取请求
     * - 定时任务等待下次执行时间
     * - 负载均衡服务等待重新平衡触发
     */
    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        // 进入等待状态
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
