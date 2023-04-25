package org.jgroups.blocks.cs.netty;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.logging.Log;
import org.jgroups.stack.IpAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.unix.Errors;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.MultithreadEventExecutorGroup;
import netty.listeners.ChannelLifecycleListener;
import netty.listeners.NettyReceiverListener;
import netty.utils.PipelineChannelInitializer;

/***
 * @author Baizel Mathew
 */
public class NettyConnection {
    private final Bootstrap clientBootstrap = new Bootstrap();
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final Map<PhysicalAddress, Channel> clientChannelMap = new ConcurrentHashMap<>();
    private final Map<PhysicalAddress, Channel> serverChannelMap = new ConcurrentHashMap<>();
    private byte[] replyAdder = null;
    private final int port;
    private final InetAddress bind_addr;
    private final EventLoopGroup boss_group; // Only handles incoming connections
    private final EventLoopGroup worker_group;
    private final NettyReceiverListener callback;
    private final ChannelLifecycleListener clientLifecycleListener;
    private final ChannelLifecycleListener serverLifecycleListener;
    private final Class<? extends ServerChannel> serverChannel;
    private final Class<? extends SocketChannel> clientChannel;
    private final Log log;

    private final int workerGroupSizeThreshold;

    private final Map<EventLoop, Set<Channel>> registeredEventLoops;
    // Protected by registeredEventLoops monitor lock - here to prevent having to count all channels for all loops
    private int totalRegisteredEventLoops;


    public NettyConnection(InetAddress bind_addr, int port, NettyReceiverListener callback, Log log,
                           EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class<? extends ServerChannel> serverChannel,
                           Class<? extends SocketChannel> clientChannel) {
        this.port = port;
        this.bind_addr = bind_addr;
        this.callback = callback;
        this.log=log;
        this.boss_group = bossGroup;
        this.worker_group = workerGroup;
        this.serverChannel = Objects.requireNonNull(serverChannel);
        this.clientChannel = Objects.requireNonNull(clientChannel);

        clientLifecycleListener = new ChannelLifecycleListener() {
            @Override
            public void channelInactive(Channel channel) {
                IpAddress ipAddress = channel.attr(ADDRESS_ATTRIBUTE).get();
                if (ipAddress != null) {
                    clientChannelMap.remove(ipAddress);
                } else {
                    clientChannelMap.values().remove(channel);
                }
            }

            @Override
            public void channelRead(Channel channel, IpAddress sender) {
                updateMap(channel, sender, false);
            }
        };

        serverLifecycleListener = new ChannelLifecycleListener() {
            @Override
            public void channelInactive(Channel channel) {
                IpAddress ipAddress = channel.attr(ADDRESS_ATTRIBUTE).get();
                if (ipAddress != null) {
                    serverChannelMap.remove(ipAddress);
                } else {
                    serverChannelMap.values().remove(channel);
                }

                synchronized (registeredEventLoops) {
                    Set<Channel> prevChannels = registeredEventLoops.get(channel.eventLoop());
                    // Must have never had a read before disconnecting
                    if (prevChannels == null) {
                        return;
                    }
                    totalRegisteredEventLoops--;
                    prevChannels.remove(channel);
                    if (prevChannels.isEmpty() && totalRegisteredEventLoops < workerGroupSizeThreshold &&
                          (totalRegisteredEventLoops - 1) >= workerGroupSizeThreshold) {
                        updateExecutors();
                    }
                }
            }

            @Override
            public void channelRead(Channel channel, IpAddress sender) {
                updateMap(channel, sender, true);
            }
        };

        int workerGroupSize;
        if (workerGroup instanceof MultithreadEventExecutorGroup) {
            workerGroupSize = ((MultithreadEventExecutorGroup) workerGroup).executorCount();
        } else {
            AtomicInteger integer = new AtomicInteger();
            workerGroup.iterator().forEachRemaining(___ -> integer.getAndIncrement());
            workerGroupSize = integer.get();
        }
        workerGroupSizeThreshold = workerGroupSize >> 1;
        registeredEventLoops = new HashMap<>(workerGroupSize);

        configureServer();
        configureClient();
    }

    private void updateExecutors() {
        Iterator<EventExecutor> iter = worker_group.iterator();
        if (totalRegisteredEventLoops <= workerGroupSizeThreshold) {
            for (Map.Entry<PhysicalAddress, Channel> entry : serverChannelMap.entrySet()) {
                boolean assigned = false;
                while (iter.hasNext()) {
                    EventLoop eventLoop = (EventLoop) iter.next();
                    if (registeredEventLoops.containsKey(eventLoop)) {
                        continue;
                    }
                    // Can only assign the new event loop in the original address event loop
                    entry.getValue().eventLoop().submit( () -> callback.updateExecutor(entry.getKey(), eventLoop));
                    assigned = true;
                    break;
                }
                if (!assigned) {
                    throw new IllegalStateException("Should never get here!");
                }
            }
        } else {
            for (Map.Entry<PhysicalAddress, Channel> entry : serverChannelMap.entrySet()) {
                // Can only assign the new event loop in the original address event loop
                entry.getValue().eventLoop().submit( () -> callback.updateExecutor(entry.getKey(), null));

            }
        }
    }

    public void run() throws InterruptedException, BindException, Errors.NativeIoException {
        serverBootstrap.bind().sync();

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
        Channel opened = clientChannelMap.getOrDefault(destAddr, null);
        if (opened != null)
            writeToChannel(opened, data, offset, length);
        else
            connectAndSend(destAddr, data, offset, length);
    }

    public static AttributeKey<IpAddress> ADDRESS_ATTRIBUTE = AttributeKey.newInstance("jgroups-ipaddress");
    // Unfortunately it is possible for write status to send two writeable messages in a row, this attribute
    // store the last status update we sent and will prevent us from sending duplicate statuses
    public static AttributeKey<Boolean> ADDRESS_WRITE_STATUS = AttributeKey.newInstance("jgroups-write-status");

    public final void connectAndSend(IpAddress addr, byte[] data, int offset, int length) {
        ChannelFuture cf = clientBootstrap.connect(new InetSocketAddress(addr.getIpAddress(), addr.getPort()));
        // Putting pack(...) inside the lambda causes unexpected behaviour.
        // Both send and receive works fine but it does not get passed up properly, might be something to do with the buffer
        ByteBuf packed = pack(cf.channel().alloc(), data, offset, length, replyAdder);
        cf.addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
                Channel ch = channelFuture.channel();
                ch.attr(ADDRESS_ATTRIBUTE).set(addr);
                ch.attr(ADDRESS_WRITE_STATUS).set(Boolean.TRUE);
                writeToChannel(ch, packed);
                updateMap(ch, addr, false);
            } else {
                log.warn("Unable to connect to " + addr, channelFuture.cause());
            }
        });
    }

    public Address getLocalAddress() {
        return new IpAddress(bind_addr, port);
    }

    public Channel getServerChannelForAddress(PhysicalAddress address, boolean server) {
        return (server ? serverChannelMap : clientChannelMap).get(address);
    }

    private void writeToChannel(Channel ch, byte[] data, int offset, int length) {
        ByteBuf packed = pack(ch.alloc(), data, offset, length, replyAdder);
        writeToChannel(ch, packed);
    }

    private static void writeToChannel(Channel ch, ByteBuf data) {
        ch.writeAndFlush(data, ch.voidPromise());
    }

    private void updateMap(Channel connected, IpAddress destAddr, boolean server) {
        Map<PhysicalAddress, Channel> map = server ? serverChannelMap : clientChannelMap;
        Channel channel = map.get(destAddr);
        if (channel != null && channel.id() == connected.id())
            return;
        log.trace("%s:%s Destination is server: %s with address %s bound to %s", bind_addr, port, server, destAddr, Thread.currentThread());
        map.put(destAddr, connected);

        if (server) {
            EventLoop eventLoop = connected.eventLoop();
            if (registeredEventLoops == null) {
                return;
            }
            synchronized (registeredEventLoops) {
                totalRegisteredEventLoops++;
                Set<Channel> prevChannels = registeredEventLoops.get(eventLoop);
                if (prevChannels != null) {
                    prevChannels.add(connected);
                } else {
                    prevChannels = new HashSet<>();
                    prevChannels.add(connected);
                    registeredEventLoops.put(eventLoop, prevChannels);
                    updateExecutors();
                }
            }
        }
    }

    private void configureClient() {
        clientBootstrap.group(worker_group)
          .handler(new PipelineChannelInitializer(this.callback, clientLifecycleListener))
          .channel(clientChannel)
          .localAddress(bind_addr, 0)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
          .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true);
    }

    private void configureServer() {
        serverBootstrap.group(boss_group, worker_group)
                .localAddress(bind_addr, port)
                .channel(serverChannel)
                .childHandler(new PipelineChannelInitializer(this.callback, serverLifecycleListener))
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
          .childOption(ChannelOption.TCP_NODELAY, true);
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

