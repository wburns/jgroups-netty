package org.jgroups.blocks.cs;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.Errors;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.jgroups.Address;
import org.jgroups.protocols.Netty;
import org.jgroups.stack.IpAddress;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/***
 * @author Baizel Mathew
 */
public class NettyServer {

    private int port;
    private InetAddress bind_addr;

    private static final int CORES = Runtime.getRuntime().availableProcessors();
    //TODO: decide the optimal amount of threads for each loop
    private EventLoopGroup boss_group; // Handles incoming connections
    private EventLoopGroup worker_group;
    private final EventExecutorGroup separateWorkerGroup = new DefaultEventExecutorGroup(16);
    private boolean isNativeTransport;
    private NettyReceiverCallback callback;
    private Bootstrap outgoingBootstrap;
    private ChannelInactiveListener inactive;
//    private NettyClient client;

    private ChannelGroup allChannels;
    private Map<IpAddress, ChannelId> ipAddressChannelIdMap;

    public NettyServer(InetAddress bind_addr, int port, NettyReceiverCallback callback, boolean isNativeTransport) {
        this.port = port;
        this.bind_addr = bind_addr;
        this.callback = callback;
        ipAddressChannelIdMap = new HashMap<>();
        allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        this.isNativeTransport = isNativeTransport;
        boss_group = isNativeTransport ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
        worker_group = isNativeTransport ? new EpollEventLoopGroup() : new NioEventLoopGroup();
        inactive = new ChannelInactiveListener() {

            @Override
            public void channelInactive(Channel channel) {
                InetSocketAddress addr = (InetSocketAddress) channel.remoteAddress();
                IpAddress ipAddress = new IpAddress(addr.getAddress(), addr.getPort());
                ChannelId id = ipAddressChannelIdMap.remove(ipAddress);
                assert id == channel.id() : "Wrong channel removed";
                allChannels.remove(channel);
            }
        };
        outgoingBootstrap = new Bootstrap();
        outgoingBootstrap.group(worker_group)
                .handler(new PipeLine(this.callback, inactive))
                .localAddress(bind_addr, 0)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true);
        if (isNativeTransport)
            outgoingBootstrap.channel(EpollSocketChannel.class);
        else
            outgoingBootstrap.channel(NioSocketChannel.class);
//    }
    }

    public Address getLocalAddress() {
        return new IpAddress(bind_addr, port);
    }

    public void shutdown() throws InterruptedException {
        boss_group.shutdownGracefully();
        worker_group.shutdownGracefully();
        separateWorkerGroup.shutdownGracefully();
    }

    public void run() throws InterruptedException, BindException, Errors.NativeIoException {
        ServerBootstrap inboundBootstrap = new ServerBootstrap();
        inboundBootstrap.group(boss_group, worker_group)
                .localAddress(bind_addr, port)
                .childHandler(new PipeLine(this.callback, inactive))
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true);
        if (isNativeTransport) {
            inboundBootstrap.channel(EpollServerSocketChannel.class);
        } else {
            inboundBootstrap.channel(NioServerSocketChannel.class);
        }
        inboundBootstrap.bind().sync();

    }

    public void send(IpAddress destAddr, byte[] data, int offset, int length) throws InterruptedException, IOException {
        byte[] packed = Netty.pack(data, offset, length, (IpAddress) getLocalAddress());
        //TODO: check if this is right behavior
        if (destAddr == null) {
            allChannels.writeAndFlush(packed);
            return;
        }
        ChannelId opened = ipAddressChannelIdMap.getOrDefault(destAddr, null);

        if (opened != null) {
            Channel writeChannel = allChannels.find(opened);
            writeChannel.eventLoop().execute(() -> {
                writeChannel.writeAndFlush(packed);
            });
        } else {
            ChannelFuture cf = outgoingBootstrap.connect(new InetSocketAddress(destAddr.getIpAddress(), destAddr.getPort()));
            cf.addListener((ChannelFutureListener) channelFuture -> {
                if (channelFuture.isSuccess()) {
                    Channel connected = channelFuture.channel();
                    updateMap(connected, destAddr);
                    connected.eventLoop().execute(() -> {
                        connected.writeAndFlush(packed);
                    });
                }
            });
        }
    }

    private void updateMap(Channel connected, IpAddress destAddr) {
        ChannelId id = ipAddressChannelIdMap.get(destAddr);
        if (id != null && id == connected.id())
            return;

        ipAddressChannelIdMap.put(destAddr, connected.id());
        allChannels.add(connected);
    }

    @ChannelHandler.Sharable
    private class ReceiverHandler extends ChannelInboundHandlerAdapter {
        private NettyReceiverCallback cb;
        private ChannelInactiveListener lifecycleListener;

        public ReceiverHandler(NettyReceiverCallback cb, ChannelInactiveListener lifecycleListener) {
            super();
            this.cb = cb;
            this.lifecycleListener = lifecycleListener;

        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            InetSocketAddress soc = (InetSocketAddress) ctx.channel().remoteAddress();
            Address sender = new IpAddress(soc.getAddress(), soc.getPort());

            ByteBuf msgbuf = (ByteBuf) msg;
            int offset = msgbuf.readInt();
            int length = msgbuf.readInt();
            int addrLen = msgbuf.readInt();
            byte[] data = new byte[length];
            byte[] addr = new byte[addrLen];
            msgbuf.readBytes(addr, 0, addrLen);
            msgbuf.readBytes(data, 0, length);

            IpAddress ad = new IpAddress();
            ad.readFrom(new DataInputStream(new ByteArrayInputStream(addr)));
            //TODO: should the sender be the client or server address from remote
            cb.onReceive(sender, data, offset, length);
            msgbuf.release();
            updateMap(ctx.channel(), ad);

        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            lifecycleListener.channelInactive(ctx.channel());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cb.onError(cause);
        }

    }

    private class PipeLine extends ChannelInitializer<SocketChannel> {
        private NettyReceiverCallback cb;
        private ChannelInactiveListener lifecycleListener;

        public final int MAX_FRAME_LENGTH = Integer.MAX_VALUE; //  not sure if this is a great idea
        public final int LENGTH_OF_FIELD = Integer.BYTES;

        public PipeLine(NettyReceiverCallback cb, ChannelInactiveListener lifecycleListener) {
            this.cb = cb;
            this.lifecycleListener = lifecycleListener;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addFirst(new FlushConsolidationHandler());//outbound and inbound (1)
            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, LENGTH_OF_FIELD, 0, LENGTH_OF_FIELD));//inbound head (2)
            ch.pipeline().addLast(new ByteArrayEncoder()); //outbound tail (3)
//            ch.pipeline().addLast(new ReceiverHandler(cb, lifecycleListener));//inbound tail (4)
            ch.pipeline().addLast(separateWorkerGroup, "handlerThread", new ReceiverHandler(cb, lifecycleListener)); // (4)
            // inbound ---> 1, 2, 4
            // outbound --> 3,1
        }
    }

    private interface ChannelInactiveListener {
        void channelInactive(Channel channel);
    }
}

