/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
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
package com.alibaba.rocketmq.remoting.netty;

import com.alibaba.rocketmq.common.SystemClock;
import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.alibaba.rocketmq.remoting.common.ServiceThread;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingSysResponseCode;
import com.alibaba.rocketmq.remoting.statistics.LatencyStatisticsItem;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.*;


/**
 * Server与Client公用抽象类
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public abstract class NettyRemotingAbstract {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

    protected static final String LATENCY_NAME_RPC = "RPC";

    protected static final String LATENCY_NAME_SEND_MESSAGE = "SendMessage";


    // 信号量，one-way情况会使用，防止本地Netty缓存请求过多
    protected final Semaphore semaphoreOneway;

    // 信号量，异步调用情况会使用，防止本地Netty缓存请求过多
    protected final Semaphore semaphoreAsync;

    protected final SystemClock systemClock = new SystemClock(1);

    // 缓存所有对外请求
    protected final ConcurrentHashMap<Integer /* opaque */, ResponseFuture> responseTable =
            new ConcurrentHashMap<Integer, ResponseFuture>(256);

    // 默认请求代码处理器
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    // 注册的各个RPC处理器
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    protected SSLContext sslContext;

    protected final ConcurrentHashMap<String, LatencyStatisticsItem> latencyMap;


    public abstract ChannelEventListener getChannelEventListener();


    public abstract RPCHook getRPCHook();


    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    class NettyEventExecutor extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int MaxSize = 10000;


        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= MaxSize) {
                this.eventQueue.add(event);
            }
            else {
                LOGGER.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(),
                        event.toString());
            }
        }


        @Override
        public void run() {
            LOGGER.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStoped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                        case IDLE:
                            listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                            break;
                        case CLOSE:
                            listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                            break;
                        case CONNECT:
                            listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                            break;
                        case EXCEPTION:
                            listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                            break;
                        default:
                            break;

                        }
                    }
                }
                catch (Exception e) {
                    LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            LOGGER.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }


    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
        if (this instanceof NettyRemotingServer) {
            this.latencyMap = new ConcurrentHashMap<>(2);
            initLatencyReport();
        } else {
            this.latencyMap = null;
        }
    }

    protected void initLatencyReport() {
        this.latencyMap.put(LATENCY_NAME_RPC, new LatencyStatisticsItem(LATENCY_NAME_RPC));
        this.latencyMap.put(LATENCY_NAME_SEND_MESSAGE, new LatencyStatisticsItem(LATENCY_NAME_SEND_MESSAGE));
    }


    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair =
                null == matched ? this.defaultRequestProcessor : matched;
        final long startTimePoint = systemClock.now();
        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        RPCHook rpcHook = NettyRemotingAbstract.this.getRPCHook();
                        if (rpcHook != null) {
                            rpcHook.doBeforeRequest(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        }

                        final RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                        if (rpcHook != null) {
                            rpcHook.doAfterResponse(cmd, response);
                        }

                        // One-way形式忽略应答结果
                        if (!cmd.isOnewayRPC()) {
                            if (response != null) {
                                response.setOpaque(cmd.getOpaque());
                                response.markResponseType();
                                try {
                                    ChannelFuture channelFuture = ctx.writeAndFlush(response);
                                    if (null != latencyMap) {
                                        channelFuture.addListener(new ChannelFutureListener() {
                                            @Override
                                            public void operationComplete(ChannelFuture future) throws Exception {
                                                long interval = systemClock.now() - startTimePoint;

                                                latencyMap.get(LATENCY_NAME_RPC).add(interval);
                                                if (cmd.getCode() == 10 || cmd.getCode() == 310 || cmd.getCode() == 36 ) {
                                                    latencyMap.get(LATENCY_NAME_SEND_MESSAGE).add(interval);
                                                }
                                            }
                                        });
                                    }
                                } catch (Throwable e) {
                                    LOGGER.error("process request over, but response failed", e);
                                    LOGGER.error(cmd.toString());
                                    LOGGER.error(response.toString());
                                }
                            }
                            else {
                                // 收到请求，但是没有返回应答，可能是processRequest中进行了应答，忽略这种情况
                            }
                        }
                    }
                    catch (Throwable e) {
                        LOGGER.error("process request exception", e);
                        LOGGER.error(cmd.toString());

                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response =
                                    RemotingCommand.createResponseCommand(
                                        RemotingSysResponseCode.SYSTEM_ERROR,//
                                        RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(cmd.getOpaque());
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            try {
                // 这里需要做流控，要求线程池对应的队列必须是有大小限制的
                pair.getObject2().submit(run);
            }
            catch (RejectedExecutionException e) {
                // 每个线程10s打印一次
                if ((System.currentTimeMillis() % 10000) == 0) {
                    LOGGER.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) //
                            + ", too many requests and system thread pool busy, RejectedExecutionException " //
                            + pair.getObject2().toString() //
                            + " request code: " + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response =
                            RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                                "too many requests and system thread pool busy, please try another server");
                    response.setOpaque(cmd.getOpaque());
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response = RemotingCommand
                    .createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(cmd.getOpaque());
            ctx.writeAndFlush(response);
            LOGGER.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }


    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final ResponseFuture responseFuture = responseTable.get(cmd.getOpaque());
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);

            responseFuture.release();

            // 异步调用
            if (responseFuture.getInvokeCallback() != null) {
                boolean runInThisThread = false;
                ExecutorService executor = this.getCallbackExecutor();
                if (executor != null) {
                    try {
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    responseFuture.executeInvokeCallback();
                                }
                                catch (Throwable e) {
                                    LOGGER.warn("execute callback in executor exception, and callback throw", e);
                                }
                            }
                        });
                    }
                    catch (Exception e) {
                        runInThisThread = true;
                        LOGGER.warn("execute callback in executor exception, maybe executor busy", e);
                    }
                }
                else {
                    runInThisThread = true;
                }

                if (runInThisThread) {
                    try {
                        responseFuture.executeInvokeCallback();
                    }
                    catch (Throwable e) {
                        LOGGER.warn("executeInvokeCallback Exception", e);
                    }
                }
            }
            // 同步调用
            else {
                responseFuture.putResponse(cmd);
            }
        } else {
            LOGGER.warn("receive response, but not matched any request, "
                    + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            LOGGER.warn(cmd.toString());
        }

        responseTable.remove(cmd.getOpaque());
    }


    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
            case REQUEST_COMMAND:
                processRequestCommand(ctx, cmd);
                break;
            case RESPONSE_COMMAND:
                processResponseCommand(ctx, cmd);
                break;
            default:
                break;
            }
        }
    }


    abstract public ExecutorService getCallbackExecutor();


    public void scanResponseTable() {
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                LOGGER.info("Removing_opaque due to timeout: opaqueId = " + next.getKey());
                it.remove();
                try {
                    rep.executeInvokeCallback();
                }
                catch (Throwable e) {
                    LOGGER.warn("scanResponseTable, operationComplete Exception", e);
                }
                finally {
                    rep.release();
                }

                LOGGER.warn("remove timeout request, " + rep);
            }
        }
    }


    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
            final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
            RemotingTimeoutException {
        try {
            final ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(), timeoutMillis, null, null);
            this.responseTable.put(request.getOpaque(), responseFuture);
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }
                    else {
                        responseFuture.setSendRequestOK(false);
                    }

                    responseTable.remove(request.getOpaque());
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    LOGGER.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                    LOGGER.warn(request.toString());
                }
            });

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                // 发送请求成功，读取应答超时
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseChannelRemoteAddr(channel),
                        timeoutMillis, responseFuture.getCause());
                }
                // 发送请求失败
                else {
                    throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel),
                        responseFuture.getCause());
                }
            }

            return responseCommand;
        }
        finally {
            this.responseTable.remove(request.getOpaque());
        }
    }


    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request,
            final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {

        long start = systemClock.now();
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        long duration = systemClock.now() - start;

        if (duration > 10) {
            LOGGER.warn("Acquire semaphore async takes {}ms", duration);
        }

        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

            final ResponseFuture responseFuture =
                    new ResponseFuture(request.getOpaque(), timeoutMillis, invokeCallback, once);
            this.responseTable.put(request.getOpaque(), responseFuture);
            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        else {
                            responseFuture.setSendRequestOK(false);
                        }

                        responseFuture.putResponse(null);
                        responseTable.remove(request.getOpaque());
                        try {
                            responseFuture.executeInvokeCallback();
                        }
                        catch (Throwable e) {
                            LOGGER.warn("execute callback in writeAndFlush addListener, and callback throw", e);
                        }
                        finally {
                            responseFuture.release();
                        }

                        LOGGER.warn("send a request command to channel <{}> failed.",
                                RemotingHelper.parseChannelRemoteAddr(channel));
                        LOGGER.warn(request.toString());
                    }
                });
            }
            catch (Exception e) {
                responseFuture.release();
                LOGGER.warn(
                        "send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel)
                                + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        }
        else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            }
            else {
                String info = String.format(
                        "invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                        timeoutMillis,//
                        this.semaphoreAsync.getQueueLength(),//
                        this.semaphoreAsync.availablePermits()//
                );
                LOGGER.warn(info);
                LOGGER.warn(request.toString());
                throw new RemotingTimeoutException(info);
            }
        }
    }


    public void invokeOnewayImpl(final Channel channel, //
                                 final RemotingCommand request, //
                                 final long timeoutMillis) //
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
            RemotingSendRequestException {
        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        once.release();
                        if (!f.isSuccess()) {
                            LOGGER.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                            LOGGER.warn(request.toString());
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                LOGGER.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                        "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                        timeoutMillis,//
                        this.semaphoreOneway.getQueueLength(),//
                        this.semaphoreOneway.availablePermits()//
                );
                LOGGER.warn(info);
                LOGGER.warn(request.toString());
                throw new RemotingTimeoutException(info);
            }
        }
    }
}
