package com.alibaba.rocketmq.cockpit;

import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameServerHandler extends SimpleChannelInboundHandler<HttpRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NameServerHandler.class);

    private static final String END_POINT = "/rocketmq/nsaddr";

    private final NameServerListController nameServerListController;

    public NameServerHandler(NameServerListController nameServerListController) {
        this.nameServerListController = nameServerListController;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, HttpRequest request) throws Exception {
        if (END_POINT.equals(request.getUri())) {
            final ByteBuf content = nameServerListController.getNameServerAddressList();
            FullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK, content);
            httpResponse.headers().add("Connection", "close");
            httpResponse.headers().add("Content-Type", "text/plain");
            httpResponse.headers().add("Content-Length", content.capacity());
            ctx.writeAndFlush(httpResponse).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    ctx.close();
                    LOGGER.info("Closing connection. Peer: {}", RemotingHelper.parseChannelRemoteAddr(future.channel()));
                    ReferenceCountUtil.release(content);
                }
            });
        } else {
            ctx.fireChannelRead(request);
        }
    }
}
