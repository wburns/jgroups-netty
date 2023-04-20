package netty.listeners;

import java.io.DataInput;

import org.jgroups.Address;

import io.netty.channel.ChannelHandlerContext;

/***
 * @author Baizel Mathew
 */
public interface NettyReceiverListener {
    void onReceive(Address sender, DataInput input) throws Exception;

    void onError(Throwable ex);

    void channelWritabilityChanged(Address outbondAddress, boolean writeable);
}