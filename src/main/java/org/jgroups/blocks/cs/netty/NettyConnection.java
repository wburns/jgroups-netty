package org.jgroups.blocks.cs.netty;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.jgroups.Address;
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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.unix.Errors;
import io.netty.util.AttributeKey;
import netty.listeners.ChannelLifecycleListener;
import netty.listeners.NettyReceiverListener;
import netty.utils.PipelineChannelInitializer;

/***
 * @author Baizel Mathew
 */
public class NettyConnection {
    private final Bootstrap clientBootstrap = new Bootstrap();
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final Map<Address, Channel> clientChannelMap = new ConcurrentHashMap<>();
    private final Map<Address, Channel> serverChannelMap = new ConcurrentHashMap<>();
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
            }

            @Override
            public void channelRead(Channel channel, IpAddress sender) {
                updateMap(channel, sender, true);
            }
        };
        configureServer();
        configureClient();
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

    public final void connectAndSend(IpAddress addr, byte[] data, int offset, int length) {
        ChannelFuture cf = clientBootstrap.connect(new InetSocketAddress(addr.getIpAddress(), addr.getPort()));
        // Putting pack(...) inside the lambda causes unexpected behaviour.
        // Both send and receive works fine but it does not get passed up properly, might be something to do with the buffer
        ByteBuf packed = pack(cf.channel().alloc(), data, offset, length, replyAdder);
        cf.addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
                Channel ch = channelFuture.channel();
                ch.attr(ADDRESS_ATTRIBUTE).set(addr);
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

    public Channel getServerChannelForAddress(Address address, boolean server) {
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
        Map<Address, Channel> map = server ? serverChannelMap : clientChannelMap;
        Channel channel = map.get(destAddr);
        if (channel != null && channel.id() == connected.id())
            return;
        log.trace("%s:%s Destination is server: %s with address %s bound to %s", bind_addr, port, server, destAddr, Thread.currentThread());
        map.put(destAddr, connected);
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

