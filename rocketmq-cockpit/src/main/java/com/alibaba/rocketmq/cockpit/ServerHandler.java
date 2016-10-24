package com.alibaba.rocketmq.cockpit;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;

public class ServerHandler extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline channelPipeline = ch.pipeline();
        channelPipeline
                .addLast(new IdleStateHandler(30, 30, 60))
                .addLast(new HttpServerCodec())
                .addLast(new NameServerHandler())
                .addLast(new DenyAllHandler());
    }
}
