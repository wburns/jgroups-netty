package netty.listeners;

import org.jgroups.Address;

/***
 * @author Baizel Mathew
 */
public interface NettyReceiverListener {
    void onReceive(Address sender, byte[] msg, int offset, int length);
    void onError(Throwable ex);
}