package org.jgroups.protocols;


import io.netty.channel.ChannelFuture;
import io.netty.channel.unix.Errors;
import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.blocks.cs.NettyClient;
import org.jgroups.blocks.cs.NettyReceiverCallback;
import org.jgroups.blocks.cs.NettyServer;
import org.jgroups.stack.IpAddress;

import java.net.BindException;

/***
 * @author Baizel Mathew
 */
public class Netty extends TP {

    private NettyClient client;
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
//            client.close();
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
        IpAddress destAddr = (IpAddress) dest;
        if (destAddr != selfAddress)
            client.send(destAddr, data, offset, length);
        else {
            //TODO: loop back
        }
    }

    private boolean createServer() throws InterruptedException {
        boolean isNative = true;
        //TODO: put the client interface inside the server to encapsulate a 'TCP connection'
        try {
            server = new NettyServer(bind_addr, bind_port, new NettyReceiverCallback() {
                @Override
                public void onReceive(Address sender, byte[] msg, int offset, int length) {
                    receive(sender, msg, offset, length);
                }

                @Override
                public void onError(Throwable ex) {
                    log.error("Error Received at Netty transport " + ex.toString());
                }
            }, isNative);
            ChannelFuture cf = server.run();
            client = new NettyClient(cf.channel().eventLoop(), bind_addr, isNative);
            selfAddress = new IpAddress(bind_addr, bind_port);
            System.out.println("created with " + bind_port);
        } catch (BindException | Errors.NativeIoException | InterruptedException exception) {
            server.shutdown();
            return false;
        }
        return true;
    }
}
