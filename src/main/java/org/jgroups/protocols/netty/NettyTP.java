package org.jgroups.protocols.netty;

import java.io.DataInput;
import java.net.BindException;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.netty.NettyConnection;
import org.jgroups.protocols.TP;
import org.jgroups.stack.IpAddress;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.unix.Errors;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import netty.listeners.NettyReceiverListener;

/***
 * @author Baizel Mathew
 */
public class NettyTP extends TP implements NettyReceiverListener {
    @Property(description = "Use Native packages when available")
    protected boolean use_native_transport;

    @Property(description = "Uses the Netty incubator (https://github.com/netty/netty-incubator-transport-io_uring) to " +
          "use IO_URING. Requires Linux with a kernel >= 5.9")
    protected boolean use_io_uring;

    private NettyConnection server;
    private IpAddress selfAddress;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public NettyTP() {
        msg_processing_policy = new NonBlockingPassRegularMessagesUpDirectly();
    }

    @Override
    public void init() throws Exception {
        super.init();
        if (!(msg_processing_policy instanceof NonBlockingPassRegularMessagesUpDirectly)) {
            log.debug("msg_processing_policy was set, ignoring as NettyTP requires it specific policy");
            msg_processing_policy = new NonBlockingPassRegularMessagesUpDirectly();
            msg_processing_policy.init(this);
        }
        initializeNettyGroupsIfNecessary();
    }

    public void replaceBossEventLoop(EventLoopGroup bossGroup) {
        if (this.bossGroup != null) {
            this.bossGroup.shutdownGracefully();
        }
        this.bossGroup = bossGroup;
    }

    public void replaceWorkerEventLoop(EventLoopGroup workerGroup) {
        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }
        this.workerGroup = workerGroup;
    }

    @Override
    public boolean supportsMulticasting() {
        return false;
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

    private void initializeNettyGroupsIfNecessary() {
        if (bossGroup == null) {
            bossGroup = createEventLoopGroup(1);
        }
        if (workerGroup == null) {
            workerGroup = createEventLoopGroup(0);
        }
    }

    protected EventLoopGroup createEventLoopGroup(int numThreads) {
        if(use_io_uring) {
            try {
                return numThreads > 0? new IOUringEventLoopGroup(numThreads) : new IOUringEventLoopGroup();
            }
            catch(Throwable t) {
                log.warn("failed to use io_uring (disabling it): " + t.getMessage());
                use_io_uring=false;
            }
        }
        if(use_native_transport) {
            try {
//                if(Util.checkForMac())
//                    return numThreads > 0? new KQueueEventLoopGroup(numThreads) : new KQueueEventLoopGroup();
                return numThreads > 0? new EpollEventLoopGroup(numThreads) : new EpollEventLoopGroup(); // Linux
            }
            catch(Throwable t) {
                log.warn("failed to use native transport", t);
                use_native_transport=false;
            }
        }
        log.debug("falling back to " + NioEventLoopGroup.class.getSimpleName());
        return numThreads > 0? new NioEventLoopGroup(numThreads) : new NioEventLoopGroup();
    }

    @Override
    public void stop() {
        try {
            server.shutdown();
        } catch (InterruptedException e) {
            log.error("Failed to shutdown server", e);
        }
        super.stop();
    }

    // Most of this method is copied from TP#receive
    @Override
    public void onReceive(Address sender, DataInput in) throws Exception {
        receive(sender, in);
    }

    @Override
    public Object down(Message msg) {
        // Don't need reliability with netty
        msg.setFlag(Message.Flag.NO_RELIABILITY);
        return super.down(msg);
    }

    PhysicalAddress toPhysicalAddress(Address address) {
        if (address instanceof PhysicalAddress) {
            return (PhysicalAddress) address;
        }
        return (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, address));
    }

    @Override
    public Object down(Event evt) {
        if (evt.type() == Event.USER_DEFINED) {
            if (evt instanceof MessageCompleteEvent) {
                Message msg = evt.getArg();
                Address physicalAddress = toPhysicalAddress(msg.src());
                Channel channel = server.getServerChannelForAddress(physicalAddress, true);
                ((NonBlockingPassRegularMessagesUpDirectly) msg_processing_policy).completedMessage(msg, channel.eventLoop());
            } else if (evt instanceof WatermarkOverflowEvent) {
                Address address = ((WatermarkOverflowEvent) evt).address();
                Address physicalAddress = toPhysicalAddress(address);

                Channel channel = server.getServerChannelForAddress(physicalAddress, true);
                channel.config().setAutoRead(!((WatermarkOverflowEvent) evt).wasOverFlow());
            }
        }
        return super.down(evt);
    }

    @Override
    public void channelWritabilityChanged(Address outbondAddress, boolean writeable) {
        // TODO: way to propagate this
    }

    @Override
    public void onError(Throwable ex) {
        log.error("error received at Netty transport ", ex);
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
            server = new NettyConnection(bind_addr, bind_port, this, log, bossGroup, workerGroup);
            server.run();
            selfAddress = (IpAddress) server.getLocalAddress();
        } catch (BindException | Errors.NativeIoException | InterruptedException exception) {
            return false;
        }
        return true;
    }
}
