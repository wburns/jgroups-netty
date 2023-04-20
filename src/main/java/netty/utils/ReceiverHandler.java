package netty.utils;

import java.io.DataInput;
import java.util.List;

import org.jgroups.blocks.cs.netty.NettyConnection;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import netty.listeners.ChannelLifecycleListener;
import netty.listeners.NettyReceiverListener;

/***
 * @author Baizel Mathew
 */
public class ReceiverHandler extends ByteToMessageDecoder {
    private final NettyReceiverListener nettyReceiverListener;
    private final ChannelLifecycleListener lifecycleListener;

    protected final Log log= LogFactory.getLog(this.getClass());
    public ReceiverHandler(NettyReceiverListener nettyReceiverListener, ChannelLifecycleListener lifecycleListener) {
        this.nettyReceiverListener = nettyReceiverListener;
        this.lifecycleListener = lifecycleListener;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msgbuf, List<Object> out) throws Exception {
        int startingPos = msgbuf.readerIndex();
        if (msgbuf.readableBytes() < 4) {
            return;
        }
        int totalLength = msgbuf.readInt();
        if (msgbuf.readableBytes() < totalLength) {
            msgbuf.readerIndex(startingPos);
            return;
        }
        // Just ignore the address length as it can parse it itself
        msgbuf.readerIndex(msgbuf.readerIndex() + 4);
        DataInput input = new ByteBufAsDataInput(msgbuf);

        IpAddress sender = new IpAddress();
        sender.readFrom(input);

        lifecycleListener.channelRead(ctx.channel(), sender);

        nettyReceiverListener.onReceive(sender, input);

        int expectedPos = startingPos + 4 + totalLength;
        if (expectedPos != msgbuf.readerIndex()) {
            log.error("Buffer was not advanced expected amount: startingPos: " + startingPos + ", totalLength: " +
                  (4 + totalLength) + ", actualPos: " + msgbuf.readerIndex());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        lifecycleListener.channelInactive(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        nettyReceiverListener.onError(cause);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        IpAddress ipAddress = ctx.channel().attr(NettyConnection.ADDRESS_ATTRIBUTE).get();
        nettyReceiverListener.channelWritabilityChanged(ipAddress, ctx.channel().isWritable());
    }
}

