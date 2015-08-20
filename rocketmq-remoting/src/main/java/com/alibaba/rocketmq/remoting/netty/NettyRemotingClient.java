/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.remoting.netty;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.exception.SSLContextCreationException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Remoting客户端实现
 *
 * <p>
 *     Version 1.1 change note:
 *     <ul>Refactor to support of multiple connections between one client and one broker.</ul>
 * </p>
 *
 * @author shijia.wxr<vintage.wang@gmail.com>, lizhanhui
 * @since 2013-7-13
 * @version 1.1
 */
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

    private static final long LockTimeoutMillis = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private final ConcurrentHashMap<String /* addr */, CompositeChannel> channelTables = new ConcurrentHashMap<String, CompositeChannel>();

    // 定时器
    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    // Name server相关
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();
    private final AtomicReference<String> namesrvAddrChosen = new AtomicReference<String>();
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());

    // 处理Callback应答器
    private final ExecutorService publicExecutor;

    private final ChannelEventListener channelEventListener;

    private RPCHook rpcHook;

    class CompositeChannel {
        private volatile int parallelism;

        private int maxParallelism;

        private final String address;

        private final List<ChannelWrapper> channelWrappers;

        private final ReentrantLock lock = new ReentrantLock();

        private final AtomicLong channelSequencer = new AtomicLong(0L);

        public CompositeChannel(String address, int maxParallelism) {
            this.address = address;
            this.maxParallelism = maxParallelism;
            channelWrappers = new ArrayList<ChannelWrapper>(maxParallelism);
        }

        public boolean containsChannel(Channel channel) {
            for (ChannelWrapper channelWrapper : channelWrappers) {
                if (null != channelWrapper && channelWrapper.getChannel() == channel) {
                    return true;
                }
            }

            return false;
        }

        public Channel createChannel() {

            if (!allowedToCreateChannel()) {
                return getChannel();
            }

            try {
                ChannelFuture channelFuture = null;
                if (lock.tryLock() || lock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                    if (!allowedToCreateChannel()) {
                        return getChannel();
                    }

                    try {
                        channelFuture = bootstrap.connect(RemotingHelper.string2SocketAddress(address));
                        log.info("createChannel: begin to connect remote host[{}] asynchronously", address);
                        ChannelWrapper channelWrapper = new ChannelWrapper(channelFuture);
                        ++parallelism;
                        channelWrappers.add(channelWrapper);
                    } finally {
                        lock.unlock();
                    }
                } else {
                    log.warn("createChannel: try to lock composite channel, but timeout, {}ms", LockTimeoutMillis);
                }

                if (null != channelFuture) {
                    if (channelFuture.awaitUninterruptibly(nettyClientConfig.getConnectTimeoutMillis(), TimeUnit.MILLISECONDS)) {
                        if (null != channelFuture.channel() && channelFuture.channel().isActive()) {
                            log.info("createChannel: connect remote host[{}] success, {}", address, channelFuture.toString());
                            return channelFuture.channel();
                        }
                    } else {
                        log.warn("createChannel: connect remote host[" + address + "] failed, " + channelFuture.toString(), channelFuture.cause());
                    }
                }
            } catch (InterruptedException e) {
                log.error("Try to lock composite channel time out.", e);
            }
            return null;
        }

        public boolean closeChannel(Channel channel) {
            if (parallelism <= 0) {
                return false;
            }
            try {
                if (lock.tryLock() || lock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {

                    if (parallelism <= 0) {
                        return false;
                    }

                    try {
                        Iterator<ChannelWrapper> iterator = channelWrappers.iterator();
                        ChannelWrapper channelWrapper;
                        while (iterator.hasNext()) {
                            channelWrapper = iterator.next();
                            if (null != channelWrapper && channelWrapper.getChannel() == channel) {
                                iterator.remove();
                                RemotingUtil.closeChannel(channel);
                                --parallelism;
                                return true;
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                log.error("Try to lock composite channel timeout.", e);
            }
            return false;
        }

        public boolean isOK() {
            for (ChannelWrapper channelWrapper : channelWrappers) {
                if (channelWrapper.isOK()) {
                    return true;
                }
            }

            return false;
        }

        public boolean isWritable() {
            for (ChannelWrapper channelWrapper : channelWrappers) {
                if (channelWrapper.isWritable()) {
                    return true;
                }
            }
            return false;
        }

        public Channel getChannel() {

            if (1 == parallelism) {
                return channelWrappers.get(0).getChannel();
            }

            if (allowedToCreateChannel()) {
                return createChannel();
            }

            ChannelWrapper channelWrapper = channelWrappers.get((int) (Math.abs(channelSequencer.getAndIncrement()) % channelWrappers.size()));

            if (channelWrapper.isOK()) {
                return channelWrapper.getChannel();
            } else {
                closeChannel(channelWrapper.getChannel());
                return createChannel();
            }
        }

        public int getParallelism() {
            return parallelism;
        }

        public String getAddress() {
            return address;
        }

        public void close() {
            lock.lock();
            try {
                Iterator<ChannelWrapper> iterator = channelWrappers.iterator();
                while (iterator.hasNext()) {
                    ChannelWrapper channelWrapper = iterator.next();
                    channelWrapper.close();
                    iterator.remove();
                    --parallelism;
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean allowedToCreateChannel() {
            return parallelism < maxParallelism;
        }
    }

    class ChannelWrapper {
        private final ChannelFuture channelFuture;


        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }


        public boolean isOK() {
            return (this.channelFuture.channel() != null && this.channelFuture.channel().isActive());
        }


        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }


        private Channel getChannel() {
            return this.channelFuture.channel();
        }


        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }

        public void close() {
            RemotingUtil.closeChannel(getChannel());
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);

        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                            SocketAddress localAddress, ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : localAddress.toString();
            final String remote = remoteAddress == null ? "UNKNOWN" : remoteAddress.toString();
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);
            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                assert remoteAddress != null;
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress.toString(), ctx.channel()));
            }
        }


        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress,
                        ctx.channel()));
            }
        }


        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress,
                        ctx.channel()));
            }
        }


        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress,
                        ctx.channel()));
            }
        }


        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE,
                                remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }
    }


    private static int initValueIndex() {
        Random r = new Random();

        return Math.abs(r.nextInt() % 999) % 999;
    }


    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }


    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,//
                               final ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOneWaySemaphoreValue(), nettyClientConfig
                .getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d",
                        this.threadIndex.incrementAndGet()));
            }
        });

        if (nettyClientConfig.isSsl()) {
            try {
                sslContext = SslHelper.getSSLContext(SslRole.CLIENT);
            } catch (SSLContextCreationException e) {
                log.error("Failed to create SSL context.", e);
            }
        }
    }


    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(//
                nettyClientConfig.getClientWorkerThreads(), //
                new ThreadFactory() {

                    private AtomicInteger threadIndex = new AtomicInteger(0);


                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                    }
                });

        final Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)//
                //
                .option(ChannelOption.TCP_NODELAY, true)
                        //
                .option(ChannelOption.SO_KEEPALIVE, false)
                        //
                .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
                        //
                .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
                        //
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        if (null == sslContext) {
                            ch.pipeline().addLast(//
                                    defaultEventExecutorGroup, //
                                    new NettyEncoder(), //
                                    new NettyDecoder(), //
                                    new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),//
                                    new NettyConnectManageHandler(), //
                                    new NettyClientHandler());
                        } else {
                            ch.pipeline().addLast(//
                                    defaultEventExecutorGroup, //
                                    new SslHandler(SslHelper.getSSLEngine(sslContext, SslRole.CLIENT)),
                                    new NettyEncoder(), //
                                    new NettyDecoder(), //
                                    new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),//
                                    new NettyConnectManageHandler(), //
                                    new NettyClientHandler());
                        }
                    }
                });

        if (nettyClientConfig.isClientPooledByteBufAllocatorEnable()) {
            // 这个选项有可能会占用大量堆外内存，暂时不使用。
            handler.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        // 每隔1秒扫描下异步调用超时情况
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Exception e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }
    }


    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            for (CompositeChannel compositeChannel : this.channelTables.values()) {
                compositeChannel.close();
                channelTables.remove(compositeChannel.getAddress());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }


    private Channel getAndCreateChannel(final String address, int parallelism) throws InterruptedException {
        if (null == address) {
            return getAndCreateNameServerChannel();
        }

        CompositeChannel compositeChannel = this.channelTables.get(address);
        if (compositeChannel == null || compositeChannel.allowedToCreateChannel()) {
            return createChannel(address, parallelism);
        }

        return compositeChannel.getChannel();
    }


    private Channel getAndCreateNameServerChannel() throws InterruptedException {
        String address = this.namesrvAddrChosen.get();
        if (address != null) {
            CompositeChannel compositeChannel = channelTables.get(address);
            if (null == compositeChannel) {
                compositeChannel = new CompositeChannel(address, 1);
                if (null == channelTables.putIfAbsent(address, compositeChannel)) {
                    return compositeChannel.createChannel();
                }
            } else {
                if (compositeChannel.allowedToCreateChannel()) {
                    return compositeChannel.createChannel();
                } else {
                    return compositeChannel.getChannel();
                }
            }
        }

        final List<String> addressList = this.namesrvAddrList.get();
        try {
            if (addressList != null && !addressList.isEmpty()) {
                for (int i = 0; i < addressList.size(); i++) {
                    int index = this.namesrvIndex.incrementAndGet();
                    index = Math.abs(index);
                    index = index % addressList.size();
                    String newAddr = addressList.get(index);
                    if (null != newAddr && !newAddr.isEmpty()) {
                        this.namesrvAddrChosen.set(newAddr);
                        return getAndCreateNameServerChannel();
                    }
                }
            }
        } catch (Exception e) {
            log.error("getAndCreateNameServerChannel: create name server channel exception", e);
        }

        return null;
    }


    private Channel createChannel(final String addr, int parallelism) throws InterruptedException {
        CompositeChannel compositeChannel = this.channelTables.get(addr);

        if (null == compositeChannel) {
            compositeChannel = new CompositeChannel(addr, parallelism);
            if (null != channelTables.putIfAbsent(addr, compositeChannel)) {
                compositeChannel = channelTables.get(addr);
            }
        }

        if (!compositeChannel.allowedToCreateChannel()) {
            return compositeChannel.getChannel();
        } else {
            return compositeChannel.createChannel();
        }
    }


    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel)
            return;

        final String addrRemote = (null == addr) ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        final CompositeChannel compositeChannel = this.channelTables.get(addrRemote);
        if (null != compositeChannel) {
            compositeChannel.closeChannel(channel);
        }
    }


    public void closeChannel(final Channel channel) {

        if (null == channel) {
            return;
        }

        try {
            CompositeChannel compositeChannel = null;
            String addrRemote = null;
            for (String key : channelTables.keySet()) {
                CompositeChannel prev = this.channelTables.get(key);
                if (null != prev && prev.containsChannel(channel)) {
                    compositeChannel = prev;
                    addrRemote = key;
                    break;
                }
            }

            if (null == compositeChannel) {
                log.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", channel);
            } else {
                compositeChannel.closeChannel(channel);
                if (compositeChannel.getParallelism() == 0) {
                    channelTables.remove(addrRemote);
                    log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                }
            }
        } catch (Exception e) {
            log.error("closeChannel: close the channel exception", e);
        }
    }


    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair =
                new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }


    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException {
        final Channel channel = this.getAndCreateChannel(addr, nettyClientConfig.getParallelism());
        if (channel != null && channel.isActive()) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis);
                if (this.rpcHook != null) {
                    this.rpcHook.doAfterResponse(request, response);
                }
                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                log.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                // 超时异常如果关闭连接可能会产生连锁反应
                // this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }


    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis,
                            InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final Channel channel = this.getAndCreateChannel(addr, nettyClientConfig.getParallelism());
        if (channel != null && channel.isActive()) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }


    @Override
    public void invokeOneWay(String addr, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException {
        final Channel channel = this.getAndCreateChannel(addr, nettyClientConfig.getParallelism());
        if (channel != null && channel.isActive()) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                this.invokeOneWayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeOneWay: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }


    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }


    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                this.namesrvAddrList.set(addrs);
            }
        }
    }


    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }


    public List<String> getNamesrvAddrList() {
        return namesrvAddrList.get();
    }


    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }


    public RPCHook getRpcHook() {
        return rpcHook;
    }


    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }


    @Override
    public RPCHook getRPCHook() {
        return this.rpcHook;
    }


    @Override
    public boolean isChannelWritable(String addr) {
        CompositeChannel compositeChannel = this.channelTables.get(addr);
        if (compositeChannel != null && compositeChannel.isOK()) {
            return compositeChannel.isWritable();
        }

        return false;
    }
}
