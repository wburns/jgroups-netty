package org.jgroups.blocks.cs;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.jgroups.Address;
import org.jgroups.stack.IpAddress;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/***
 * @author Baizel Mathew
 */
public class NettyServer {

    private int port;
    private InetAddress bind_addr;


    private EventLoopGroup boss_group;
    private EventLoopGroup worker_group;
    private NettyReceiverCallback callback;
    private ChannelInitializer<SocketChannel> channel_initializer;

    public NettyServer(InetAddress bind_addr, int port, NettyReceiverCallback callback) {

        this.port = port;
        this.bind_addr = bind_addr;
        this.callback = callback;
        channel_initializer = new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(
                        new ByteArrayDecoder(),
                        new ByteArrayEncoder(),
                        new ReceiverHandler());
            }
        };
    }

    public void shutdown() throws InterruptedException {
        boss_group.shutdownGracefully().sync();
        worker_group.shutdownGracefully().sync();
    }

    public void run() throws InterruptedException, BindException {
        boss_group = new NioEventLoopGroup();
        worker_group = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(boss_group, worker_group)
                .channel(NioServerSocketChannel.class)
                .localAddress(bind_addr, port)
                .childHandler(channel_initializer)
//                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true);
//                .childOption(ChannelOption.SO_KEEPALIVE, true);

        b.bind().sync();

    }

    private class ReceiverHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            InetSocketAddress soc = (InetSocketAddress) ctx.channel().remoteAddress();
            Address sender = new IpAddress(soc.getAddress(), soc.getPort());
            byte[] bMsg = (byte[]) msg;
            ByteBuffer buf = ByteBuffer.wrap(bMsg);
            int offset = buf.getInt();
            int length = buf.getInt();
            byte[] data = new byte[length];

            buf.get(data, 0, length);
            callback.onReceive(sender, data, offset, length);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            callback.onError(cause);
        }

    }

    public Address getLocalAddress() {
        return new IpAddress(bind_addr, port);
    }
}

