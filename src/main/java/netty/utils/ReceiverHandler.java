package netty.utils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import netty.listeners.ChannelLifecycleListener;
import netty.listeners.NettyReceiverListener;
import org.jgroups.stack.IpAddress;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

@ChannelHandler.Sharable
public class ReceiverHandler extends ChannelInboundHandlerAdapter {
    private NettyReceiverListener nettyReceiverListener;
    private ChannelLifecycleListener lifecycleListener;
    private byte[] buffer = new byte[65200];

    public ReceiverHandler(NettyReceiverListener nettyReceiverListener, ChannelLifecycleListener lifecycleListener) {
        super();
        this.nettyReceiverListener = nettyReceiverListener;
        this.lifecycleListener = lifecycleListener;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgbuf = (ByteBuf) msg;
        int totalLength = msgbuf.readInt();
        int addrLen = msgbuf.readInt();
        int dataLen = totalLength - Integer.BYTES - addrLen;

        msgbuf.readBytes(buffer, 0, addrLen);
        msgbuf.readBytes(buffer, addrLen, dataLen);

        IpAddress sender = new IpAddress();
        sender.readFrom(new DataInputStream(new ByteArrayInputStream(buffer, 0, addrLen)));
        synchronized (this) {
            nettyReceiverListener.onReceive(sender, buffer, addrLen, dataLen);
        }
        msgbuf.release();
        lifecycleListener.channelRead(ctx.channel(),sender);

    }
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        lifecycleListener.channelInactive(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        nettyReceiverListener.onError(cause);
    }

}

