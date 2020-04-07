package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;

public class Netty extends TP {

    //<--------------------------- Properties ------------------------------>
    @Property(description = "The multicast port used for sending and receiving packets. Default is 7600",
            systemProperty = Global.UDP_MCAST_PORT, writable = false)
    protected int mcast_port = 7600;
    //<--------------------------- End Properties --------------------------->


    @Override
    public boolean supportsMulticasting() {
        return true;
    }

    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {

    }

    @Override
    public String getInfo() {
        return null;
    }

    @Override
    protected PhysicalAddress getPhysicalAddress() {
        return null;
    }

    @Override
    public void start() throws Exception {

    }


}
