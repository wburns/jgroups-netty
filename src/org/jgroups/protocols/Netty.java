package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.blocks.cs.NettyClient;
import org.jgroups.blocks.cs.NettyReceiverCallback;
import org.jgroups.blocks.cs.NettyServer;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.net.BindException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Netty extends TP {

    private NettyClient client;
    private NettyServer server;
    protected final Map<Address, IpAddress> addr_table = new ConcurrentHashMap<>();


    @Override
    public boolean supportsMulticasting() {
        return false;
    }

    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        for (IpAddress addr : addr_table.values()) {
            client.send(addr, data, offset, length);
        }
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        client.send((IpAddress) dest, data, offset, length);
    }

    @Override
    public String getInfo() {
        return server.getLocalAddress().toString();
    }

    @Override
    protected PhysicalAddress getPhysicalAddress() {
        return server != null ? (PhysicalAddress) server.getLocalAddress() : null;
    }

//    public Object down(Message msg) {
//        try {
//            return _down(msg);
//        } catch (Exception e) {
//            log.error("failure passing message down", e);
//            return null;
//        }
//    }

//    protected Object _down(Message msg) throws Exception {
//        Address dest = msg.getDest();
//        setSourceAddress(msg); // very important !! listToBuffer() will fail with a null src address !!
//        int size = msg.size();
//        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(size + Global.INT_SIZE);
//        boolean isUnicast = dest != null;
//        Util.writeMessage(msg,out,!isUnicast);
//
//        if (isUnicast) // unicast
//            sendTo(dest, out.buffer(), 0, out.position());
//        else { // multicast
//            Collection<Address> dests = view != null ? view.getMembers() : addr_table.keySet();
//            for (Address dst : dests) {
//                try {
//                    sendTo(dst, out.buffer(), 0, out.position());
//                } catch (Throwable t) {
//                    log.error("failed sending multicast message to " + dst, t);
//                }
//            }
//        }
//        return null;
//    }


//    protected void sendTo(Address dest, byte[] buffer, int offset, int length) throws Exception {
//        if (local_addr == dest)
//            //TODO: Should this loopback?
//            return;
//
//        IpAddress physical_dest = null;
//
//        if (dest instanceof IpAddress)
//            physical_dest = (IpAddress) dest;
//        else
//            physical_dest = addr_table.get(dest);
//        if (physical_dest == null)
//            throw new Exception(String.format("physical address for %s not found", dest));
//
//        client.send(physical_dest, buffer, offset, length);
//    }

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
            client = new NettyClient(bind_addr, bind_port+50);
        else
            throw new BindException("No port found to bind within port range");
//        log.debug("Start Port info  " + bind_port + " addr " + bind_addr + " port " + port_range);
        super.start();
    }

//    public Object down(Event evt) {
//        Object retval = super.down(evt);
//        switch (evt.type()) {
//            case Event.ADD_PHYSICAL_ADDRESS:
//                Tuple<Address, PhysicalAddress> tuple = evt.arg();
//                IpAddress val = (IpAddress) tuple.getVal2();
//                addr_table.put(tuple.getVal1(), val);
//                break;
//            case Event.VIEW_CHANGE:
//
//                for (Iterator<Map.Entry<Address, IpAddress>> it = addr_table.entrySet().iterator(); it.hasNext(); ) {
//                    Map.Entry<Address, IpAddress> entry = it.next();
//                    if (!view.containsMember(entry.getKey())) {
//                        IpAddress sock_addr = entry.getValue();
//                        it.remove();
//                        //TODO: close connection
//                    }
//                }
//                break;
//        }
//        return retval;
//    }

    private boolean createServer() {
        try {
            server = new NettyServer(bind_addr, bind_port, new NettyReceiverCallback() {
                @Override
                public void onReceive(Address sender, byte[] msg, int offset, int length) {
//                    log.error("Received at Netty transport FROM "+ sender);
                    receive(sender, msg, offset, length);
                }

                @Override
                public void onError(Throwable ex) {
                    log.error("Error Received at Netty transport " + ex.toString());
                }
            });
            server.run();
        } catch (BindException exception) {
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


}
