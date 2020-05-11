package org.jgroups.blocks.cs;

import org.jgroups.Address;

/***
 * @author Baizel Mathew
 */
public interface NettyReceiverCallback {
    void onReceive(Address sender, byte[] msg, int offset, int length);
    void onError(Throwable ex);
}
