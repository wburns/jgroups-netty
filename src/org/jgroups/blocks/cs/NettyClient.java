package org.jgroups.blocks.cs;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NettyClient {
    protected final Log log = LogFactory.getLog(this.getClass());

    private EventLoopGroup group;
    ChannelGroup connections;
    private Map<SocketAddress, ChannelId> channelIds;
    private InetAddress local_addr;
    private int local_port;

    public NettyClient(InetAddress local_addr, int port) {
        channelIds = new HashMap<>();
        connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        group = new NioEventLoopGroup();

        this.local_addr = local_addr;
        this.local_port = port;

    }

    private void close() throws InterruptedException {
        group.shutdownGracefully().sync();
    }

    public void send(IpAddress dest, byte[] data, int offset, int length) throws InterruptedException {
        send(dest.getIpAddress(), dest.getPort(), data, offset, length);
    }

    public void send(InetAddress remote_addr, int remote_port, byte[] data, int offset, int length) throws InterruptedException {
        InetSocketAddress dest = new InetSocketAddress(remote_addr, remote_port);
        Channel ch = connect(dest);
        if (ch != null) {
            byte[] packedData = pack(data, offset, length);
//            log.error("Packed data - Len "+ length + " ACtualv datbuf len "+ data.length + " Send to "+ ch.remoteAddress() + " from "+ ch.localAddress());
            ch.writeAndFlush(packedData).sync();
        }
    }

    private byte[] pack(byte[] data, int offset, int length) {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + data.length);
        buf.putInt(offset);
        buf.putInt(length);
        buf.put(data);
        return buf.array();
    }

    private Channel connect(InetSocketAddress remote_addr) throws InterruptedException {
        if (!channelIds.containsKey(remote_addr)) {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .localAddress(local_addr,local_port)
                    .remoteAddress(remote_addr)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,2000) // Wait 2 seconds for conn
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new ByteArrayEncoder());
                            ch.pipeline().addLast(new ByteArrayDecoder());
                            ch.pipeline().addLast(new ClientHandler());
                        }
                    });
            ChannelFuture cf = b.connect();
            cf.awaitUninterruptibly();
            if (cf.isSuccess())
                return cf.channel();
            return null;
        }
        return connections.find(channelIds.get(remote_addr));
    }


    class ClientHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            SocketAddress addr = ctx.channel().remoteAddress();
            log.error("Channel Active and added " + addr);
            connections.add(ctx.channel());
            channelIds.put(addr, ctx.channel().id());
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
//            log.error((byte[]) msg + " Not suspposed to receive messages here");
            //TODO: handle message
        }
    }
}
