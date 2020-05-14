package netty.utils;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import netty.listeners.ChannelLifecycleListener;
import netty.listeners.NettyReceiverListener;

public class PipelineChannelInitializer extends ChannelInitializer<SocketChannel> {
    private NettyReceiverListener nettyReceiverListener;
    private ChannelLifecycleListener lifecycleListener;
    private EventExecutorGroup separateWorkerGroup;

    public final int MAX_FRAME_LENGTH = Integer.MAX_VALUE; //  not sure if this is a great idea
    public final int LENGTH_OF_FIELD = Integer.BYTES;

    public PipelineChannelInitializer(NettyReceiverListener nettyReceiverListener, ChannelLifecycleListener lifecycleListener, EventExecutorGroup separateWorkerGroup) {
        this.nettyReceiverListener = nettyReceiverListener;
        this.lifecycleListener = lifecycleListener;
        this.separateWorkerGroup=separateWorkerGroup;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline().addFirst(new FlushConsolidationHandler(1000 * 32, true));//outbound and inbound (1)
        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, LENGTH_OF_FIELD, 0, LENGTH_OF_FIELD));//inbound head (2)
        ch.pipeline().addLast(separateWorkerGroup, "handlerThread", new ReceiverHandler(nettyReceiverListener, lifecycleListener)); // (4)
        // inbound ---> 1, 2, 4
        // outbound --> 1
    }
}
