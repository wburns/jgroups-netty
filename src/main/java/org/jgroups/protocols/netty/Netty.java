package org.jgroups.protocols.netty;

import io.netty.channel.unix.Errors;
import org.jgroups.blocks.cs.netty.NettyReceiverCallback;
import org.jgroups.blocks.cs.netty.NettyServer;
import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.TP;
import org.jgroups.stack.IpAddress;

import java.net.BindException;

/***
 * @author Baizel Mathew
 */
public class Netty extends TP {
    @Property(description = "Use Native packages when available")
    protected boolean use_native_transport;

    private NettyServer server;
    private IpAddress selfAddress = null;


    @Override
    public boolean supportsMulticasting() {
        return false;
    }

    @Override
    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        sendToMembers(members, data, offset, length);
    }

    @Override
    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        _send(dest, data, offset, length);
    }

    @Override
    public String getInfo() {
        return null;
    }

    @Override
    public void start() throws Exception {
        boolean isServerCreated = createServer();
        while (!isServerCreated && bind_port < bind_port + port_range) {
            //Keep trying to create server until
            bind_port++;
            isServerCreated = createServer();
            //TODO: Fix this to get valid port numbers
        }
        if (!isServerCreated)
            throw new BindException("No port found to bind within port range");
        super.start();
    }

    @Override
    public void stop() {
        try {
            server.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.error("Failed to shutdown server");
        }
        super.stop();

    }

    @Override
    protected PhysicalAddress getPhysicalAddress() {
        return server != null ? (PhysicalAddress) server.getLocalAddress() : null;
    }

    private void _send(Address dest, byte[] data, int offset, int length) throws Exception {
        IpAddress destAddr = dest != null ? (IpAddress) dest : null;

        if (destAddr != selfAddress) {
            server.send(destAddr, data, offset, length);

        } else {
            //TODO: loop back
        }
    }

    private boolean createServer() throws InterruptedException {
        try {
            server = new NettyServer(bind_addr, bind_port, new NettyReceiverCallback() {
                @Override
                public void onReceive(Address sender, byte[] msg, int offset, int length) {
                    //This method is called from a non IO thread. it should be safe for this to block without affecting netty receive
                    synchronized (this) {
                        receive(sender, msg, offset, length);
                    }
                }

                @Override
                public void onError(Throwable ex) {
                    log.error("Error Received at Netty transport " + ex.toString());
                }
            }, use_native_transport);
            server.run();
            selfAddress = (IpAddress) server.getLocalAddress();
        } catch (BindException | Errors.NativeIoException | InterruptedException exception) {
            server.shutdown();
            return false;
        }
        return true;
    }

}
