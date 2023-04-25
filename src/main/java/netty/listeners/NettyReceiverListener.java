package netty.listeners;

import java.io.DataInput;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;

/***
 * @author Baizel Mathew
 */
public interface NettyReceiverListener {
    void onReceive(Address sender, DataInput input) throws Exception;

    void onError(Throwable ex);

    void channelWritabilityChanged(Address outbondAddress, boolean writeable);

    // Updates the address event loop executor. Note this method must be invoked in the event loop for the given address
    void updateExecutor(PhysicalAddress address, EventLoop eventLoop);
}