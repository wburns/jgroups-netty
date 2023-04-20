package org.jgroups.protocols.netty;

import java.io.DataInput;
import java.net.BindException;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.netty.NettyConnection;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.TP;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.MemberAvailabilityEvent;
import org.jgroups.util.MessageCompleteEvent;
import org.jgroups.util.NettyAsyncHeader;
import org.jgroups.util.NonBlockingPassRegularMessagesUpDirectly;
import org.jgroups.util.Util;
import org.jgroups.util.WatermarkOverflowEvent;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.Errors;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;
import io.netty.incubator.channel.uring.IOUringSocketChannel;
import io.netty.util.concurrent.FastThreadLocal;
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

    private Class<? extends ServerChannel> serverChannel;
    private Class<? extends SocketChannel> clientChannel;

    public NettyTP() {
        msg_processing_policy = new NonBlockingPassRegularMessagesUpDirectly();
    }

    public NettyConnection getServer() {
        return server;
    }

    @Override
    public void init() throws Exception {
        ClassConfigurator.addIfAbsent(NettyAsyncHeader.MAGIC_ID, NettyAsyncHeader.class);

        super.init();
        if (serverChannel == null) {
            serverChannel = serverChannel();
        }
        if (clientChannel == null) {
            clientChannel = clientChannel();
        }
        if (!(msg_processing_policy instanceof NonBlockingPassRegularMessagesUpDirectly)) {
            log.debug("msg_processing_policy was set, ignoring as NettyTP requires it specific policy");
            msg_processing_policy = new NonBlockingPassRegularMessagesUpDirectly();
            msg_processing_policy.init(this);
        }
    }

    public void replaceBossEventLoop(EventLoopGroup bossGroup) {
        if (server != null) {
            throw new IllegalStateException("Boss event loop group cannot be set after server has been started!");
        }
        if (this.bossGroup != null) {
            this.bossGroup.shutdownGracefully();
        }
        this.bossGroup = bossGroup;
    }

    public void replaceWorkerEventLoop(EventLoopGroup workerGroup) {
        if (server != null) {
            throw new IllegalStateException("Worker event loop group cannot be set after server has been started!");
        }
        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }
        this.workerGroup = workerGroup;
    }

    public void setServerChannel(Class<? extends ServerChannel> serverChannel) {
        if (server != null) {
            throw new IllegalStateException("Server channel cannot be set after server has been started!");
        }
        this.serverChannel = serverChannel;
    }

    public void setSocketChannel(Class<? extends SocketChannel> clientChannel) {
        if (server != null) {
            throw new IllegalStateException("Client channel cannot be set after server has been started!");
        }
        this.clientChannel = clientChannel;
    }

    @Override
    public boolean supportsMulticasting() {
        return false;
    }

    @Override
    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        IpAddress destAddr = dest != null ? (IpAddress) dest : null;

        if (destAddr != selfAddress) {
            server.send(destAddr, isOOB.get(), data, offset, length);

        } else {
            //TODO: loop back
        }
    }

    @Override
    public String getInfo() {
        return null;
    }

    @Override
    public void start() throws Exception {
        initializeNettyGroupsIfNecessary();
        // We have to set this before actually being connected, because it is possible the server/client may
        // get a request before we assign the server reference
        selfAddress = new IpAddress(bind_addr, bind_port);
        boolean isServerCreated = createServer();
        while (!isServerCreated && bind_port < bind_port + port_range) {
            //Keep trying to create server until
            bind_port++;
            selfAddress = new IpAddress(bind_addr, bind_port);
            isServerCreated = createServer();
            //TODO: Fix this to get valid port numbers
        }
        if (!isServerCreated) {
            selfAddress = null;
            throw new BindException("No port found to bind within port range");
        }
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
        // Shut down without a quiet period
        bossGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS);
        workerGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS);
        super.stop();
    }

    // Most of this method is copied from TP#receive
    @Override
    public void onReceive(Address sender, DataInput in) throws Exception {
        receive(sender, in);
    }

    public PhysicalAddress toPhysicalAddress(Address address) {
        if (address instanceof PhysicalAddress) {
            return (PhysicalAddress) address;
        }
        return (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, address));
    }

    private final static FastThreadLocal<Boolean> isOOB = new FastThreadLocal<>();
    @Override
    public Object down(Message msg) {
        isOOB.set(msg.isFlagSet(Message.Flag.OOB));

        return super.down(msg);
    }

    @Override
    public Object down(Event evt) {
        Object retVal = super.down(evt);
        switch (evt.getType()) {
            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                ((NonBlockingPassRegularMessagesUpDirectly)msg_processing_policy).viewChange(view.getMembers());
                break;
            case Event.USER_DEFINED:
                if (evt instanceof MessageCompleteEvent) {
                    Message msg = evt.getArg();
                    ((NonBlockingPassRegularMessagesUpDirectly) msg_processing_policy).completedMessage(msg);
                } else if (evt instanceof WatermarkOverflowEvent) {
                    Address address = ((WatermarkOverflowEvent) evt).address();
                    Address physicalAddress = toPhysicalAddress(address);

                    Channel channel = server.getServerChannelForAddress(physicalAddress, true);
                    channel.config().setAutoRead(!((WatermarkOverflowEvent) evt).wasOverFlow());
                }
                break;
        }
        return retVal;
    }

    @Override
    public void channelWritabilityChanged(PhysicalAddress outboundAddress, boolean writeable) {
        if (is_trace) {
            if (writeable) {
                log.trace("%s Member %s is available for writing, sending event up to notify user to continue", addr(), outboundAddress);
            } else {
                log.trace("%s Member %s is no longer available for writing, sending event up to notify user to reduce pressure", addr(), outboundAddress);
            }
        }
        Address logicalAddress = logical_addr_cache.getByValue(outboundAddress);
        up(new MemberAvailabilityEvent(logicalAddress, writeable));
    }

    @Override
    public void onError(Throwable ex) {
        log.error("error received at Netty transport ", ex);
    }

    @Override
    protected PhysicalAddress getPhysicalAddress() {
        return selfAddress;
    }

    private boolean createServer() {
        try {
            server = new NettyConnection(bind_addr, bind_port, this, log, bossGroup, workerGroup,
                  serverChannel, clientChannel);
            server.run();
        } catch (BindException | Errors.NativeIoException | InterruptedException exception) {
            return false;
        }
        return true;
    }
    protected Class<? extends ServerChannel> serverChannel() {
        if(use_io_uring)
            return IOUringServerSocketChannel.class;
        if(use_native_transport) {
            if(Util.checkForMac())
                return KQueueServerSocketChannel.class;
            return EpollServerSocketChannel.class;
        }
        return NioServerSocketChannel.class;
    }

    protected Class<? extends SocketChannel> clientChannel() {
        if(use_io_uring)
            return IOUringSocketChannel.class;
        if(use_native_transport) {
            if(Util.checkForMac())
                return KQueueSocketChannel.class;
            return EpollSocketChannel.class;
        }
        return NioSocketChannel.class;
    }
}
