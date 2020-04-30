package org.jgroups.blocks.cs;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/***
 * @author Baizel Mathew
 */
public class NettyServer {

    private int port;
    private InetAddress bind_addr;

    protected Log log = LogFactory.getLog(getClass());

    private EventLoopGroup boss_group;
    private EventLoopGroup worker_group;
    private EventLoopGroup separateWorkerGroup;

    private NettyReceiverCallback callback;
    private ChannelInitializer<SocketChannel> channel_initializer;

    public NettyServer(InetAddress bind_addr, int port, NettyReceiverCallback callback, int MAX_FRAME_LENGTH, int LENGTH_OF_FIELD) {
        this.port = port;
        this.bind_addr = bind_addr;
        this.callback = callback;
        separateWorkerGroup = new NioEventLoopGroup();

        channel_initializer = new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addFirst(new FlushConsolidationHandler());
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, LENGTH_OF_FIELD, 0, LENGTH_OF_FIELD));
                ch.pipeline().addLast(separateWorkerGroup, "handlerThread", new ReceiverHandler());
            }
        };
    }

    public void shutdown() throws InterruptedException {
        boss_group.shutdownGracefully().sync();
        worker_group.shutdownGracefully().sync();
        separateWorkerGroup.shutdownGracefully();
    }

    public void run() throws InterruptedException, BindException {
        int cores = Runtime.getRuntime().availableProcessors();
        boss_group = new NioEventLoopGroup(cores);
        worker_group = new NioEventLoopGroup(2 * cores - 1);

        ServerBootstrap b = new ServerBootstrap();
        b.group(boss_group, worker_group)
                .channel(NioServerSocketChannel.class)
                .localAddress(bind_addr, port)
                .childHandler(channel_initializer)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(32 * 1024, 64 * 1024));
        b.bind().sync();

    }


    private class ReceiverHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            int offset = msg.readInt();
            int length = msg.readInt();
            byte[] data = new byte[length];
            msg.readBytes(data);
            InetSocketAddress soc = (InetSocketAddress) ctx.channel().remoteAddress();
            Address sender = new IpAddress(soc.getAddress(), soc.getPort());
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

