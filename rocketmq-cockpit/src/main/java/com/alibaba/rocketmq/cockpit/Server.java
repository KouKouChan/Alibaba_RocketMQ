package com.alibaba.rocketmq.cockpit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private EventLoopGroup bossEventLoopGroup;
    private EventLoopGroup workerEventLoopGroup;
    private int port;

    private final NameServerListController nameServerListController;

    public Server(int port) {
        bossEventLoopGroup = new NioEventLoopGroup(1);
        workerEventLoopGroup = new NioEventLoopGroup();
        if (port > 0) {
            this.port = port;
        } else {
            this.port = 80;
        }

        nameServerListController = new NameServerListController("/dianyi/conf");
    }

    public void launch() {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossEventLoopGroup, workerEventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(1024 * 64, 1024 * 256, 1024 * 1024))
                    .childOption(ChannelOption.SO_SNDBUF, 1024 * 512)
                    .childHandler(new ServerHandler(nameServerListController));
            ChannelFuture channelFuture = bootstrap.bind(port).sync();
            LOGGER.info("Server started, listening {}", port);
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            LOGGER.error("Server failed to start", e);
        } finally {
            if (null != bossEventLoopGroup) {
                bossEventLoopGroup.shutdownGracefully();
            }

            if (null != workerEventLoopGroup) {
                workerEventLoopGroup.shutdownGracefully();
            }
        }

    }

    public static void main(String[] args) throws ParseException {

        Options options = new Options();
        options.addOption("p", "port", true, "port to bind, default to 80");
        int port = 0;
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("p")) {
            port = Integer.parseInt(commandLine.getOptionValue("p"));
        } else {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("rocketmq-cockpit", options);
        }

        Server server = new Server(port);
        server.launch();
    }

}
