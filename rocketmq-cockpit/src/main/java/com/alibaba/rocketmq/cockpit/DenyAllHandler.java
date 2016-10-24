package com.alibaba.rocketmq.cockpit;

import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;


public class DenyAllHandler extends SimpleChannelInboundHandler<HttpRequest> {

    private static final String ACCESS_DENIED_MSG = "Access Denied";

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, HttpRequest msg) throws Exception {
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(ACCESS_DENIED_MSG.getBytes());
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.UNAUTHORIZED, byteBuf);
        response.headers().add("Content-Type", "text/plain");
        response.headers().add("Content-Length", byteBuf.capacity());
        response.headers().add("Connection", "close");
        ctx.writeAndFlush(response).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                RemotingUtil.closeChannel(ctx.channel());
                ReferenceCountUtil.release(byteBuf);
            }
        });
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}
