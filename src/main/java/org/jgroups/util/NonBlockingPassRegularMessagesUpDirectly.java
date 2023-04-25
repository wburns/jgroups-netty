package org.jgroups.util;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.MsgStats;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.TpHeader;
import org.jgroups.protocols.netty.NettyTP;

import io.netty.channel.EventLoop;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.util.internal.shaded.org.jctools.queues.SpscLinkedQueue;

/**
 * This class is very similar to {@link MaxOneThreadPerSender} and in fact many of the code is copied from there.
 * The big difference is that it will never block the invoking thread.
 * The general idea is that non oob messages will be processed in order, as usual, but we rely upon external notifications
 * to tell us that the message was completed via
 */
public class NonBlockingPassRegularMessagesUpDirectly extends SubmitToThreadPool {
   protected final ConcurrentMap<Address, Entry> senderTable = new ConcurrentHashMap<>();

   public void viewChange(List<Address> members) {
      senderTable.keySet().retainAll(members);
   }

   public void destroy() {
      senderTable.clear();
   }

   @Override
   public void reset() {
      senderTable.values().forEach(Entry::reset);
   }

   protected NettyTP transport;

   public void init(NettyTP transport) {
      super.init(transport);
      if (low_watermark <= 0) {
         log.debug("msg_processing_policy.low_watermark was set 0 or less, reverting to default of " + DEFAULT_LOW_WATER_MARK);
         low_watermark = DEFAULT_LOW_WATER_MARK;
      }
      if (high_watermark <= 0) {
         log.debug("msg_processing_policy.high_watermark was set 0 or less, reverting to default of " + DEFAULT_HIGH_WATER_MARK);
         high_watermark = DEFAULT_HIGH_WATER_MARK;
      }
      this.transport = transport;
   }

   @Override
   public void init(TP transport) {
      if (!(transport instanceof NettyTP)) {
         throw new UnsupportedOperationException("Only support NettyTP transports!");
      }
      init((NettyTP) transport);
   }

   private static final int DEFAULT_LOW_WATER_MARK = 32 * 1024;
   private static final int DEFAULT_HIGH_WATER_MARK = 64 * 1024;

   @Property(description="When pending non oob messages from sender are reduced below this after previous exceeding high_watermark will allow reads to become unblocked." +
         " Must be greater than 0, defaults to " + DEFAULT_LOW_WATER_MARK)
   protected int                low_watermark = DEFAULT_LOW_WATER_MARK;
   @Property(description="When pending non oob messages from sender exceed this amount, additional reads will be stopped until byte total is less than low_watermark." +
         " Must be greater than 0, defaults to " + DEFAULT_HIGH_WATER_MARK)
   protected int                high_watermark = DEFAULT_HIGH_WATER_MARK;

   @ManagedOperation(description="Dumps unicast and multicast tables")
   public String dump() {
      return String.format("\nsenderTable:\n%s", mapToString(senderTable));
   }

   static String mapToString(ConcurrentMap<Address, Entry> map) {
      return map.values().stream()
            .map(Object::toString)
            .collect(Collectors.joining("\n", "", ""));
   }

   public void completedMessage(Message msg) {
      Entry entry = senderTable.get(msg.getSrc());
      if (entry != null) {
         log.trace("%s Marking %s as completed", tp.addr(), msg);
         entry.messageCompleted(msg);
      } else {
         log.debug("%s Message %s was marked as completed, but was not present in MessageTable, most likely concurrent stop", tp.addr(), msg);
      }
   }

   @Override
   public boolean loopback(Message msg, boolean oob) {
      if(oob)
         return super.loopback(msg, oob);
      throw new UnsupportedOperationException("Loopback regular messages are not supported with this TP yet");
   }

   @Override
   public boolean process(MessageBatch batch, boolean oob) {
      if (oob) {
         return super.process(batch, true);
      }
      Entry entry = senderTable.computeIfAbsent(batch.sender(), Entry::new);
      return entry.process(batch);
   }

   @Override
   public boolean process(Message msg, boolean oob) {
      if (oob) {
         return super.process(msg, true);
      }
      Entry entry = senderTable.computeIfAbsent(msg.getSrc(), Entry::new);
      return entry.process(msg);
   }

   // This method must be executed on the event loop thread for the given address
   public boolean updateExecutor(PhysicalAddress src, EventLoop eventLoop) {
      for (Entry entry : senderTable.values()) {
         if (src.equals(transport.toPhysicalAddress(entry.sender))) {
            assert entry.ourEventLoop.inEventLoop();
            updateEntryEventLoop(entry, eventLoop);
            return true;
         }
      }
      log.trace("%s No sender registered matching %s for event loop update, waiting until created", transport.addr(), src);
      return false;
   }

   private void updateEntryEventLoop(Entry entry, EventLoop eventLoop) {
      entry.eventLoopToExecuteOn = eventLoop;
      Object threadObj = eventLoop instanceof SingleThreadEventLoop ? ((SingleThreadEventLoop) eventLoop).threadProperties().name() : eventLoop;
      log.trace("%s Updating processing handler to %s for target %s", transport.addr(), threadObj, entry.sender);
   }

   private static final AtomicLongFieldUpdater<Entry> SUBMITTED_MSGS_UPDATER = AtomicLongFieldUpdater.newUpdater(Entry.class, "submitted_msgs");
   private static final AtomicLongFieldUpdater<Entry> QUEUED_MSGS_UPDATER = AtomicLongFieldUpdater.newUpdater(Entry.class, "queued_msgs");

   protected class Entry implements Runnable {
      volatile Thread runningThread = null;
      // This variable is only ever written to from the sender event loop. However, it can be read from any thread, but
      // we guarantee it can not happen concurrently
      protected final SpscLinkedQueue<Message> batch;    // used to queue messages
      protected final EventLoop ourEventLoop;
      protected final Address      sender;

      protected volatile long               submitted_msgs;
      protected volatile long               queued_msgs;

      // Needs to be volatile as we can read it from a different thread on completion
      protected volatile Message   messageBeingProcessed;
      protected long batchLength;
      protected EventLoop eventLoopToExecuteOn;
      protected volatile boolean sentOverFlow;

      protected Entry(Address sender) {
         this.sender=sender;
         batch=new SpscLinkedQueue<>();

         PhysicalAddress physicalAddress = transport.toPhysicalAddress(sender);

         this.ourEventLoop = transport.getServer().getServerChannelForAddress(physicalAddress, true).eventLoop();

         EventLoop eventLoop = transport.getPendingExecutorUpdates().remove(physicalAddress);
         updateEntryEventLoop(this, eventLoop);
      }


      public Entry reset() {
         submitted_msgs=queued_msgs=0;
         return this;
      }

      // This method may be called from any thread
      protected void messageCompleted(Message msg) {
         if (msg != messageBeingProcessed) {
            log.error("%s Inconsistent message completed %s versus processing %s, this is most likely a bug!", tp.addr(), msg, messageBeingProcessed);
         }
         if (Thread.currentThread() == runningThread) {
            log.trace("%s Message %s completed synchronously ", tp.addr(), msg);
            messageBeingProcessed = null;
            return;
         }
         log.trace("%s Message %s completed async, dispatching next message if applicable", tp.addr(), msg);

         // NOTE: we cannot set messageBeingProcessed to null as it was completed asynchronously, because if there is a
         // pending read between our submission below that it enqueues the message

         run();
      }

      /**
       * Submits the single message and returns whether the message was processed synchronously or not
       * @param msg the message to send up
       * @return whether the message completed synchronously
       */
      protected boolean submitMessage(Message msg, boolean forceSend) {
         // Following block is just copied from SubmitToThreadPool#SingleMessageHandler instead of allocating a new
         // object and also because the constructor is protected
         {
            try {
               if (eventLoopToExecuteOn == null || eventLoopToExecuteOn.inEventLoop()) {
                  actualSend(msg);
               } else if (forceSend) {
                  eventLoopToExecuteOn.submit(() -> actualSend(msg));
               } else {
                  batch.add(msg);
                  eventLoopToExecuteOn.submit(this);
                  log.trace("%s Message %s added to batch and submitted to event loop", tp.addr(), msg);
                  return false;
               }
            }
            catch(Throwable t) {
               log.error(Util.getMessage("PassUpFailure"), t);
            }

         }
         // Check for the presence of the async header to tell if message may be delayed
         // It won't have the header yet if ran on a different event loop
         if (!(msg.getHeader(tp.getId()) instanceof NettyAsyncHeader) && eventLoopToExecuteOn == null) {
            messageBeingProcessed = null;
         } else if (messageBeingProcessed != null) {
            log.trace("%s Message %s not completed synchronously, must wait until it is complete later", tp.addr(), msg);
            return false;
         }
         return true;
      }

      private void actualSend(Message msg) {
         if(tp.statsEnabled()) {
            sendStats(msg);
         }

         TpHeader hdr=msg.getHeader(tp_id);
         Address dest=msg.getDest();
         runningThread = Thread.currentThread();
         tp.passMessageUp(msg, hdr.getClusterName(), true, dest == null, true);
         runningThread = null;
      }

      private void sendStats(Message msg) {
         MsgStats msg_stats=tp.getMessageStats();
         boolean oob=msg.isFlagSet(Message.Flag.OOB);
         if(oob)
            msg_stats.incrNumOOBMsgsReceived(1);
         else
            msg_stats.incrNumMsgsReceived(1);
         msg_stats.incrNumBytesReceived(msg.getLength());
      }

      private void processBatchAsync() {
         log.trace("%s Processing batch for %s", tp.addr(), sender);

      }

      public boolean process(Message msg) {
         assert ourEventLoop.inEventLoop();
         if (messageBeingProcessed != null) {
            QUEUED_MSGS_UPDATER.incrementAndGet(this);
            batch.add(msg);
            notifyOnWatermarkOverflow(msg.getSrc());
            return false;
         }
         SUBMITTED_MSGS_UPDATER.incrementAndGet(this);
         messageBeingProcessed = msg;
         return submitMessage(msg, false);
      }

      public boolean process(MessageBatch batch) {
         assert ourEventLoop.inEventLoop();
         if (messageBeingProcessed != null) {
            QUEUED_MSGS_UPDATER.addAndGet(this, batch.size());
            batch.transferFrom(batch, false);
            notifyOnWatermarkOverflow(batch.sender());
            return false;
         }
         int submittedAmount = 0;
         Iterator<Message> iter = batch.iterator();
         while (iter.hasNext()) {
            Message msg = iter.next();

            submittedAmount++;
            messageBeingProcessed = msg;
            if (!submitMessage(msg, false)) {
               break;
            }
         }
         SUBMITTED_MSGS_UPDATER.addAndGet(this, submittedAmount);
         int queuedAmount = 0;
         while (iter.hasNext()) {
            Message msg = iter.next();
            queuedAmount++;
            batch.add(msg);
         }
         QUEUED_MSGS_UPDATER.addAndGet(this, queuedAmount);
         notifyOnWatermarkOverflow(batch.sender());
         return false;
      }

      private void notifyOnWatermarkOverflow(Address sender) {
         assert ourEventLoop.inEventLoop();
         if (batchLength < high_watermark) {
            long newBatchLength = batchLength();
            log.trace("%s Batch size has increased from %s to %s for sender %s", tp.addr(), batchLength, newBatchLength, sender);
            batchLength = newBatchLength;
            if (batchLength > high_watermark) {
               log.trace("%s High watermark met for sender %s, pausing reads", tp.addr(), sender);
               tp.down(new WatermarkOverflowEvent(sender, true));
               sentOverFlow = true;
            }
         }
      }

      // unsynchronized on batch but who cares
      public String toString() {
         return String.format("batch size=%d queued msgs=%d submitted msgs=%d",
               batch.size(), queued_msgs, submitted_msgs);
      }

      protected long batchLength() {
         assert ourEventLoop.inEventLoop();
         long size = 0;
         for (Message message : batch) {
            size += message.getLength();
         }
         return size;
      }

      // This code can only be invoked in the event loop for this sender
      @Override
      public void run() {
         assert messageBeingProcessed != null;

         Message msg = batch.poll();
         if (msg == null) {
            log.trace("%s Batch was empty for %s", tp.addr(), sender);
         }
         log.trace("%s Processing batch for %s", tp.addr(), sender);

         int processedAmount = 0;
         do {
            messageBeingProcessed = msg;
            if (!submitMessage(msg, true)) {
               break;
            }
            processedAmount++;
         } while ((msg = batch.poll()) != null);
         // Sent all messages from the batch, need to send update to ourEventLoop that it may need to stop batching
         if (msg == null) {
            if (ourEventLoop.inEventLoop()) {
               log.trace("%s Batch for sender %s was completed", tp.addr(), sender);
               messageBeingProcessed = null;
            } else {
               log.trace("%s Batch for sender %s may be complete, attempting", tp.addr(), sender);
               ourEventLoop.submit(this::attemptStopBatching);
            }
            return;
         }
         if (sentOverFlow) {
            long endingLength = batchLength();
            log.trace("%s Processed %d messages for %s, new batch size is %d", tp.addr(), processedAmount, sender, endingLength);
            if (endingLength < low_watermark) {
               if (ourEventLoop.inEventLoop()) {
                  checkAfterBatchComplete(endingLength);
               } else {
                  // Note we have to recalculate the batch length again inside the event loop as it writes to the
                  // batch, so it is the only way to get a consistent size since there could be outstanding reads from the
                  // socket even after disabling auto read
                  ourEventLoop.execute(this::checkAfterBatchComplete);
               }
            }
         } else {
            log.trace("%s Processed %d messages for %s from batch", tp.addr(), processedAmount, sender);
         }
      }

      private void attemptStopBatching() {
         assert ourEventLoop.inEventLoop();
         if (batch.isEmpty()) {
            log.trace("%s Batch for sender %s is now empty", tp.addr(), sender);
            messageBeingProcessed = null;
         } else {
            log.trace("%s Batch for sender %s still has entries", tp.addr(), sender);
         }
      }

      private void checkAfterBatchComplete() {
         long endingLength = batchLength();
         checkAfterBatchComplete(endingLength);
      }

      private void checkAfterBatchComplete(long endingLength) {
         assert ourEventLoop.inEventLoop();
         batchLength = endingLength;
         if (sentOverFlow && endingLength < low_watermark) {
            log.trace("%s Low watermark met for %s, resuming reads", tp.addr(), sender);
            tp.down(new WatermarkOverflowEvent(sender, false));
            sentOverFlow = false;
         }
      }
   }
}
