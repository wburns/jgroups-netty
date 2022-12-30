package org.jgroups.blocks.cs.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.Errors;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;
import io.netty.incubator.channel.uring.IOUringSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import netty.listeners.ChannelLifecycleListener;
import netty.listeners.NettyReceiverListener;
import netty.utils.PipelineChannelInitializer;
import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/***
 * @author Baizel Mathew
 */
public class NettyConnection {

    private final EventExecutorGroup separateWorkerGroup = new DefaultEventExecutorGroup(4);
    private final Bootstrap outgoingBootstrap = new Bootstrap();
    private final ServerBootstrap inboundBootstrap = new ServerBootstrap();
    private final Map<IpAddress, Channel> ipAddressChannelMap = new HashMap<>();
    private byte[] replyAdder = null;
    private final int port;
    private final InetAddress bind_addr;
    private final EventLoopGroup boss_group; // Only handles incoming connections
    private final EventLoopGroup worker_group;
    private boolean isNativeTransport;
    private boolean useIOURing;
    private final NettyReceiverListener callback;
    private final ChannelLifecycleListener lifecycleListener;
    private final Log log;

    public NettyConnection(InetAddress bind_addr, int port, NettyReceiverListener callback, Log log,
                           boolean isNativeTransport, boolean useIOURing) {
        this.port = port;
        this.bind_addr = bind_addr;
        this.callback = callback;
        this.log=log;
        this.isNativeTransport = isNativeTransport;
        this.useIOURing = useIOURing;
        boss_group = createEventLoopGroup(1);
        worker_group = createEventLoopGroup(0);

        lifecycleListener = new ChannelLifecycleListener() {
            @Override
            public void channelInactive(Channel channel) {
                ipAddressChannelMap.values().remove(channel);
            }

            @Override
            public void channelRead(Channel channel, IpAddress sender) {
                updateMap(channel, sender);
            }
        };
        configureClient();
        configureServer();
    }

    public void run() throws InterruptedException, BindException, Errors.NativeIoException {
        inboundBootstrap.bind().sync();

        try {
            ByteArrayOutputStream replyAddByteStream = new ByteArrayOutputStream();
            DataOutputStream dStream = new DataOutputStream(replyAddByteStream);
            new IpAddress(bind_addr, port).writeTo(dStream);
            replyAdder = replyAddByteStream.toByteArray();
        } catch (IOException e) {
            //Nodes will have to use two channels per connection
            e.printStackTrace();
        }
    }

    public final void send(IpAddress destAddr, byte[] data, int offset, int length) {
        Channel opened = ipAddressChannelMap.getOrDefault(destAddr, null);
        if (opened != null)
            writeToChannel(opened, data, offset, length);
        else
            connectAndSend(destAddr, data, offset, length);
    }

    public final void connectAndSend(IpAddress addr, byte[] data, int offset, int length) {
        ChannelFuture cf = outgoingBootstrap.connect(new InetSocketAddress(addr.getIpAddress(), addr.getPort()));
        // Putting pack(...) inside the lambda causes unexpected behaviour.
        // Both send and receive works fine but it does not get passed up properly, might be something to do with the buffer
        ByteBuf packed = pack(cf.channel().alloc(), data, offset, length, replyAdder);
        cf.addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
                Channel ch = channelFuture.channel();
                writeToChannel(ch, packed);
                updateMap(ch, addr);
            }
        });
    }

    public final void connectAndSend(IpAddress addr) {
        //Send an empty message so receiver knows reply addr. otherwise Receiver will make another connection
        connectAndSend(addr, null, 0, 0);
    }

    public Address getLocalAddress() {
        return new IpAddress(bind_addr, port);
    }

    public void shutdown() throws InterruptedException {
        boss_group.shutdownGracefully();
        worker_group.shutdownGracefully();
        separateWorkerGroup.shutdownGracefully();
    }

    private void writeToChannel(Channel ch, byte[] data, int offset, int length) {
        ByteBuf packed = pack(ch.alloc(), data, offset, length, replyAdder);
        writeToChannel(ch, packed);
    }

    private static void writeToChannel(Channel ch, ByteBuf data) {
        ch.eventLoop().execute(() -> ch.writeAndFlush(data, ch.voidPromise()));
    }

    private void updateMap(Channel connected, IpAddress destAddr) {
        Channel channel = ipAddressChannelMap.get(destAddr);
        if (channel != null && channel.id() == connected.id())
            return;

        if (channel != null) {
            //if we already have a connection and then this will only be true in one of the nodes thus only closing one connection instead of two
            if (connected.remoteAddress().equals(new InetSocketAddress(destAddr.getIpAddress(), destAddr.getPort()))) {
                connected.close();
            }
            return;
        }
        ipAddressChannelMap.put(destAddr, connected);
    }

    protected EventLoopGroup createEventLoopGroup(int numThreads) {
        if(useIOURing) {
            try {
                return numThreads > 0? new IOUringEventLoopGroup(numThreads) : new IOUringEventLoopGroup();
            }
            catch(Throwable t) {
                log.warn("failed to use io_uring (disabling it): " + t.getMessage());
                useIOURing=false;
            }
        }
        if(isNativeTransport) {
            try {
                if(Util.checkForMac())
                    return numThreads > 0? new KQueueEventLoopGroup(numThreads) : new KQueueEventLoopGroup();
                return numThreads > 0? new EpollEventLoopGroup(numThreads) : new EpollEventLoopGroup(); // Linux
            }
            catch(Throwable t) {
                log.warn("failed to use native transport", t);
                isNativeTransport=false;
            }
        }
        log.debug("falling back to " + NioEventLoopGroup.class.getSimpleName());
        return numThreads > 0? new NioEventLoopGroup(numThreads) : new NioEventLoopGroup();
    }

    protected Class<? extends Channel> channelClass(boolean server) {
        if(useIOURing)
            return server? IOUringServerSocketChannel.class : IOUringSocketChannel.class;
        if(isNativeTransport) {
            if(Util.checkForMac())
                return server? KQueueServerSocketChannel.class : KQueueSocketChannel.class;
            return server? EpollServerSocketChannel.class : EpollSocketChannel.class;
        }
        return server? NioServerSocketChannel.class : NioSocketChannel.class;
    }



    private void configureClient() {
        outgoingBootstrap.group(worker_group)
          .handler(new PipelineChannelInitializer(this.callback, lifecycleListener, separateWorkerGroup))
          .localAddress(bind_addr, 0)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
          .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true);
        outgoingBootstrap.channel(channelClass(false));
    }

    private void configureServer() {
        inboundBootstrap.group(boss_group, worker_group)
                .localAddress(bind_addr, port)
                .childHandler(new PipelineChannelInitializer(this.callback, lifecycleListener, separateWorkerGroup))
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
          .childOption(ChannelOption.TCP_NODELAY, true);
        Class<? extends Channel> ch=channelClass(true);
        inboundBootstrap.channel((Class<? extends ServerChannel>)ch);
    }

    private static ByteBuf pack(ByteBufAllocator allocator, byte[] data, int offset, int length, byte[] replyAdder) {
        int allocSize = Integer.BYTES + length + Integer.BYTES + replyAdder.length;
        ByteBuf buf = allocator.buffer(allocSize);
        // size of data + size replyAddr.length field  + space for reply addr bytes = total frame size
        buf.writeInt(length + replyAdder.length + Integer.BYTES);  //encode frame size and data length
        buf.writeInt(replyAdder.length);
        buf.writeBytes(replyAdder);
        if (data != null)
            buf.writeBytes(data, offset, length);
        return buf;
    }
}

