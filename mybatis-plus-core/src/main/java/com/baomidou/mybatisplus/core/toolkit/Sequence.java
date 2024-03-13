/*
 * Copyright (c) 2011-2023, baomidou (jobob@qq.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.baomidou.mybatisplus.core.toolkit;

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 分布式高效有序 ID 生产黑科技(sequence)
 *
 * <p>优化开源项目：https://gitee.com/yu120/sequence</p>
 *
 * @author hubin
 * @since 2016-08-18
 */
public class Sequence {

    private static final Log logger = LogFactory.getLog(Sequence.class);
    /**
     * 时间起始标记点，作为基准，一般取系统的最近时间（一旦确定不能变动）
     */
    private final long twepoch;
    private static final long DEFAULT_TWEPOCH=1288834974657L;
    /**
     * 机器标识位数
     */
    private final long workerIdBits = 5L;
    private final long datacenterIdBits = 5L;
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);
    private final long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);
    /**
     * 毫秒内自增位
     */
    private final long sequenceBits = 12L;
    private final long workerIdShift = sequenceBits;
    private final long datacenterIdShift = sequenceBits + workerIdBits;
    /**
     * 时间戳左移动位
     */
    private final long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    private final long workerId;

    /**
     * 数据标识 ID 部分
     */
    private final long datacenterId;
    /**
     * 并发控制
     */
    private long sequence = 0L;
    /**
     * 上次生产 ID 时间戳
     */
    private long lastTimestamp = -1L;
    /**
     * IP 地址
     */
    private InetAddress inetAddress;
    /**
     * 最大时间偏移量,并发不大的系统可增大该值,增大时间回滚容错性.
     */
    private final long maxTimeOffset;
    /**
     * 启用SystemClock功能来获取时间,并发不是极大情况下可关闭此功能,避免频繁线程上下文切换
     */
    private final boolean enableSystemClock;

    public Sequence(){
        this(null,0,0,null,true,5);
    }

    public Sequence(InetAddress inetAddress) {
        this(inetAddress,0,0,null,true,5);
    }

    /**
     * 有参构造器
     *
     * @param workerId     工作机器 ID
     * @param datacenterId 序列号
     */
    public Sequence(long workerId, long datacenterId) {
        this(null,workerId,datacenterId,null,true,5);
    }

    public Sequence(LocalDateTime startTime) {
        this(null,0,0,startTime,true,5);
    }

    public Sequence(LocalDateTime startTime,boolean _enableSystemClock,long _maxTimeOffset) {
        this(null,0,0,startTime,_enableSystemClock,_maxTimeOffset);
    }
    /**
     * 构造函数
     * @param workerId 工作机器 ID
     * @param datacenterId 数据中心ID
     * @param startTime 启始时间,null使用默认启始时间,默认启动时间2010开始会浪费很多id
     * @param _enableSystemClock 启用SystemClock功能来获取时间
     * @param _maxTimeOffset 最大时间容错偏移量
     */
    public Sequence(InetAddress inetAddress,long workerId, long datacenterId, LocalDateTime startTime,
                    boolean _enableSystemClock,long _maxTimeOffset) {
        enableSystemClock=_enableSystemClock;
        maxTimeOffset=_maxTimeOffset<=0?5:_maxTimeOffset;
        if (inetAddress!=null){
            this.inetAddress=inetAddress;
        }
        if(datacenterId<=0){
            this.datacenterId = getDatacenterId(maxDatacenterId);
        }else {
            Assert.isFalse(datacenterId > maxDatacenterId ,
                String.format("datacenter Id can't be greater than %d", maxDatacenterId));
            this.datacenterId =datacenterId;
        }
        if(workerId<=0){
            this.workerId = getMaxWorkerId(datacenterId, maxWorkerId);
        }else {
            Assert.isFalse(workerId > maxWorkerId,
                String.format("worker Id can't be greater than %d", maxWorkerId));
            this.workerId =workerId;
        }

        if (startTime!=null) {
            twepoch=startTime.atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        }else{
            twepoch=DEFAULT_TWEPOCH;
        }
        initLog();
    }

    private void initLog() {
        if (logger.isDebugEnabled()) {
            logger.debug("Initialization Sequence datacenterId:" + this.datacenterId + " workerId:" + this.workerId);
        }
    }

    /**
     * 获取 maxWorkerId
     */
    protected long getMaxWorkerId(long datacenterId, long maxWorkerId) {
        StringBuilder mpid = new StringBuilder();
        mpid.append(datacenterId);
        String name = ManagementFactory.getRuntimeMXBean().getName();
        if (StringUtils.isNotBlank(name)) {
            /*
             * GET jvmPid
             */
            mpid.append(name.split(StringPool.AT)[0]);
        }
        /*
         * MAC + PID 的 hashcode 获取16个低位
         */
        return (mpid.toString().hashCode() & 0xffff) % (maxWorkerId + 1);
    }

    /**
     * 数据标识id部分
     */
    protected long getDatacenterId(long maxDatacenterId) {
        long id = 0L;
        try {
            if (null == this.inetAddress) {
                this.inetAddress = InetAddress.getLocalHost();
            }
            NetworkInterface network = NetworkInterface.getByInetAddress(this.inetAddress);
            if (null == network) {
                id = 1L;
            } else {
                byte[] mac = network.getHardwareAddress();
                if (null != mac) {
                    id = ((0x000000FF & (long) mac[mac.length - 2]) | (0x0000FF00 & (((long) mac[mac.length - 1]) << 8))) >> 6;
                    id = id % (maxDatacenterId + 1);
                }
            }
        } catch (Exception e) {
            logger.warn(" getDatacenterId: " + e.getMessage());
        }
        return id;
    }

    /**
     * 获取下一个 ID
     *
     * @return 下一个 ID
     */
    public synchronized long nextId() {
        long timestamp = timeGen();
        //闰秒
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            if (offset <= maxTimeOffset) {
                try {
                    wait(offset << 1);
                    timestamp = timeGen();
                    if (timestamp < lastTimestamp) {
                        throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", offset));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", offset));
            }
        }

        if (lastTimestamp == timestamp) {
            // 相同毫秒内，序列号自增
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // 同一毫秒的序列数已经达到最大
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            // 不同毫秒内，序列号置为 1 - 9 随机数
            sequence = ThreadLocalRandom.current().nextLong(1, 9);
        }

        lastTimestamp = timestamp;

        // 时间戳部分 | 数据中心部分 | 机器标识部分 | 序列号部分
        return ((timestamp - twepoch) << timestampLeftShift)
            | (datacenterId << datacenterIdShift)
            | (workerId << workerIdShift)
            | sequence;
    }

    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    protected long timeGen() {
        return enableSystemClock? SystemClock.now():System.currentTimeMillis();
    }

    /**
     * 反解id的时间戳部分
     */
    public long parseIdTimestamp(long id) {
        return (id>>22)+twepoch;
    }
}
