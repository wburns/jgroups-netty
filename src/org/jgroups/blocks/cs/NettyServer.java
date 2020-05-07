package org.jgroups.blocks.cs;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.unix.Errors;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.jgroups.Address;
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

    private static final int CORES = Runtime.getRuntime().availableProcessors();
    //TODO: decide the optimal amount of threads for each loop
    private EventLoopGroup boss_group = new EpollEventLoopGroup(); // Handles incoming connections
    private EventLoopGroup worker_group = new EpollEventLoopGroup();
    private final EventExecutorGroup separateWorkerGroup = new DefaultEventExecutorGroup(16);

    private NettyReceiverCallback callback;

    public NettyServer(InetAddress bind_addr, int port, NettyReceiverCallback callback) {
        this.port = port;
        this.bind_addr = bind_addr;
        this.callback = callback;
    }

    public void shutdown() throws InterruptedException {
        boss_group.shutdownGracefully();
        worker_group.shutdownGracefully();
        separateWorkerGroup.shutdownGracefully();
    }

    public void run() throws InterruptedException, BindException, Errors.NativeIoException {
        //TODO: add the option to use native transport for Unix machines
        //https://netty.io/wiki/native-transports.html
        ServerBootstrap b = new ServerBootstrap();
        b.group(boss_group, worker_group)
                .channel(EpollServerSocketChannel.class)
                .localAddress(bind_addr, port)
                .childHandler(new ChannelInit(this.callback))
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(200, 128 * 1024, 512 * 1024))
                .childOption(ChannelOption.TCP_NODELAY, true)
//                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(128 * 1024, 512 * 1024));
        b.bind().sync();

    }

    @ChannelHandler.Sharable
    private class ReceiverHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private NettyReceiverCallback cb;

        public ReceiverHandler(NettyReceiverCallback cb) {
            this.cb = cb;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            int offset = msg.readInt();
            int length = msg.readInt();
            byte[] data = new byte[length];
            msg.readBytes(data);
            InetSocketAddress soc = (InetSocketAddress) ctx.channel().remoteAddress();
            Address sender = new IpAddress(soc.getAddress(), soc.getPort());
            cb.onReceive(sender, data, offset, length);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cb.onError(cause);
        }

    }

    private class ChannelInit extends ChannelInitializer<SocketChannel> {
        private NettyReceiverCallback cb;
        public final int MAX_FRAME_LENGTH = Integer.MAX_VALUE; //  not sure if this is a great idea
        public final int LENGTH_OF_FIELD = Integer.BYTES;

        public ChannelInit(NettyReceiverCallback callback) {
            cb = callback;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, LENGTH_OF_FIELD, 0, LENGTH_OF_FIELD));
            //Its own thread so it wont block IO thread
            ch.pipeline().addLast(separateWorkerGroup, "handlerThread", new ReceiverHandler(cb));
        }
    }

    public Address getLocalAddress() {
        return new IpAddress(bind_addr, port);
    }

}

