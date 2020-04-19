package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.blocks.cs.NettyClient;
import org.jgroups.blocks.cs.NettyReceiverCallback;
import org.jgroups.blocks.cs.NettyServer;
import org.jgroups.stack.IpAddress;

import java.net.BindException;
import java.util.Collection;

/***
 * @author Baizel Mathew
 */
public class Netty extends BasicTCP {

    private NettyClient client;
    private NettyServer server;

    @Override
    public void start() throws Exception {
        boolean isServerCreated = createServer();
        while (!isServerCreated && bind_port < bind_port + port_range) {
            //Keep trying to create server until
            bind_port++;
            isServerCreated = createServer();
            //TODO: Fix this to get valid port numbers
        }
        if (isServerCreated)
            client = new NettyClient(bind_addr, bind_port + 50);
        else
            throw new BindException("No port found to bind within port range");
        super.start();
    }

    @Override
    public void stop() {
        try {
            server.shutdown();
            client.close();
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

    @Override
    public String printConnections() {
        return null;
    }

    @Override
    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        client.send((IpAddress) dest, data, offset, length);
    }

    @Override
    public void retainAll(Collection<Address> members) {
        //TODO
    }

    private boolean createServer() throws InterruptedException {
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
            });
            server.run();
        } catch (BindException exception) {
            server.shutdown();
            return false;
        } catch (InterruptedException e) {
            server.shutdown();
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
