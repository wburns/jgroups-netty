//package org.jgroups.blocks.cs.netty;
//
//import io.netty.bootstrap.Bootstrap;
//import io.netty.buffer.PooledByteBufAllocator;
//import io.netty.channel.*;
//import io.netty.channel.epoll.EpollServerDomainSocketChannel;
//import io.netty.channel.epoll.EpollSocketChannel;
//import io.netty.channel.group.ChannelGroup;
//import io.netty.channel.group.DefaultChannelGroup;
//import io.netty.channel.socket.SocketChannel;
//import io.netty.channel.socket.SocketChannelConfig;
//import io.netty.channel.socket.nio.NioSocketChannel;
//import io.netty.handler.codec.bytes.ByteArrayDecoder;
//import io.netty.handler.codec.bytes.ByteArrayEncoder;
//import io.netty.handler.flush.FlushConsolidationHandler;
//import io.netty.util.concurrent.FutureListener;
//import io.netty.util.concurrent.GlobalEventExecutor;
//import org.jgroups.protocols.netty.Netty;
//import org.jgroups.stack.IpAddress;
//
//import java.io.IOException;
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.net.SocketAddress;
//import java.nio.ByteBuffer;
//import java.util.HashMap;
//import java.util.Map;
//
///***
// * @author Baizel Mathew
// */
//public class NettyClient {
////    protected final Log log = LogFactory.getLog(this.getClass());
//
//    ChannelGroup connections;
//    private Map<SocketAddress, ChannelId> channelIds;
//    private Bootstrap bootstrap;
//
//    public NettyClient(EventLoopGroup group, InetAddress local_addr, int max_timeout_interval, boolean isNativeTransport) {
//
//        channelIds = new HashMap<>();
//        connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
//
//        bootstrap = new Bootstrap();
//        bootstrap.group(group)
//                .localAddress(local_addr, 0)
//                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, max_timeout_interval)
////                .option(ChannelOption.SO_REUSEADDR, true)
////                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(32 * 1024, 64 * 1024))
////                .option(ChannelOption.SO_SNDBUF,64*1024) // default is 16 * 1024
//                .option(ChannelOption.SO_RCVBUF,0) // default is 128 * 1024
//                .option(ChannelOption.AUTO_READ,false)
//                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
//                .option(ChannelOption.TCP_NODELAY, true)
//                .handler(new ChannelInitializer<SocketChannel>() {
//                    @Override
//                    protected void initChannel(SocketChannel ch) throws Exception {
//                        ch.pipeline().addFirst(new FlushConsolidationHandler(512));
//                        ch.pipeline().addLast(new ByteArrayEncoder());
//                        ch.pipeline().addLast(new ByteArrayDecoder());
//                        ch.pipeline().addLast(new ClientHandler());
//                    }
//                });
//        if (isNativeTransport)
//            bootstrap.channel(EpollSocketChannel.class);
//        else
//            bootstrap.channel(NioSocketChannel.class);
//    }
//
//    public NettyClient(EventLoopGroup group, InetAddress local_addr, boolean isNativeTransport) {
//        this(group, local_addr, 1000, isNativeTransport);
//    }
//
//    public Bootstrap getBootstrap(){
//        return bootstrap;
//    }
////    public void send(IpAddress dest, byte[] data, int offset, int length) throws InterruptedException {
////        send(dest.getIpAddress(), dest.getPort(), data, offset, length);
////    }
//
////    public void send(InetAddress remote_addr, int remote_port, byte[] data, int offset, int length) throws InterruptedException {
////        InetSocketAddress dest = new InetSocketAddress(remote_addr, remote_port);
////        Channel ch = connect(dest);
////        if (ch != null && ch.isOpen()) {
////            byte[] packedData = Netty.pack(data, offset, length);
////            ch.eventLoop().execute(() -> {
////                ch.writeAndFlush(packedData, ch.voidPromise());
////            });
////        }
////    }
//
//
////    public Channel connect(InetSocketAddress remote_addr) throws InterruptedException {
////        ChannelId chId = channelIds.get(remote_addr);
////        if (chId != null)
////            return connections.find(chId);
////
////        ChannelFuture cf = bootstrap.connect(remote_addr);
////        cf.addListener(
////                (ChannelFutureListener)
////                        future1 -> {
////                            if (future1.isSuccess()) {
////                            } else {
////                            }
////                        });
////        cf.awaitUninterruptibly(); // Wait max_timeout_interval seconds for conn
////        if (cf.isDone()) {
////            Channel ch = cf.channel();
////            if (cf.isSuccess()) {
////                connections.add(ch);
////                channelIds.put(remote_addr, ch.id());
////                return ch;
////            } else {
////                ch.close().sync();
////            }
////        }
////        return null;
////    }
//
////    @ChannelHandler.Sharable
////    private class ClientHandler extends ChannelInboundHandlerAdapter {
////
////        @Override
////        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
////            Channel ch = ctx.channel();
////            connections.remove(ch);
////            channelIds.remove(ch.remoteAddress());
////            ch.close();
////        }
////
////        @Override
////        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
////            if (!(cause instanceof IOException)) {
//////                log.error("Error caught in client " + cause.toString());
////                cause.printStackTrace();
////            }
////        }
////
////    }
//}
