package org.jgroups.blocks.cs;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

/***
 * @author Baizel Mathew
 */
public class NettyServer {

    private int port;
    private InetAddress bind_addr;

    protected Log log = LogFactory.getLog(getClass());

    private EventLoopGroup boss_group;
    private EventLoopGroup worker_group;

//    private EventLoopGroup sharedEventLoop;

    private NettyReceiverCallback callback;
    private ChannelInitializer<SocketChannel> channel_initializer;

    public NettyServer(InetAddress bind_addr, int port, NettyReceiverCallback callback) {
        this.port = port;
        this.bind_addr = bind_addr;
        this.callback = callback;
        EventLoopGroup serprateGroup = new NioEventLoopGroup();

        channel_initializer = new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(
                        new Decoder(),
                        new ByteArrayEncoder());
                ch.pipeline().addLast(serprateGroup, "handler", new ReceiverHandler());
            }
        };
    }

    public void shutdown() throws InterruptedException {
        boss_group.shutdownGracefully().sync();
        worker_group.shutdownGracefully().sync();
//        sharedEventLoop.shutdownGracefully();
    }

    public void run() throws InterruptedException, BindException {
        boss_group = new NioEventLoopGroup(1);
        worker_group = new NioEventLoopGroup(2);

        ServerBootstrap b = new ServerBootstrap();
        b.group(boss_group, worker_group)
                .channel(NioServerSocketChannel.class)
                .localAddress(bind_addr, port)
                .childHandler(channel_initializer)
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        b.bind().sync();

    }

    @ChannelHandler.Sharable
    private class ReceiverHandler extends SimpleChannelInboundHandler<byte[]> {
        @Override
        public void channelRead0(ChannelHandlerContext ctx, byte[] msg) throws Exception {
            InetSocketAddress soc = (InetSocketAddress) ctx.channel().remoteAddress();
            Address sender = new IpAddress(soc.getAddress(), soc.getPort());
//            byte[] bMsg = (byte[]) msg;
            int offset = fromByteArray(msg, 0);
            int length = fromByteArray(msg, Integer.BYTES);
            byte[] data = readNBytes(msg, Integer.BYTES * 2, length);

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

    private byte[] readNBytes(byte[] data, int offset, int length) {
        return Arrays.copyOfRange(data, offset, length + offset);
    }

    private int fromByteArray(byte[] data, int offset) {
        return ((data[offset + 0] & 0xFF) << 24) |
                ((data[offset + 1] & 0xFF) << 16) |
                ((data[offset + 2] & 0xFF) << 8) |
                ((data[offset + 3] & 0xFF) << 0);
    }
}

