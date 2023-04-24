package org.jgroups.util;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.MsgStats;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.TpHeader;
import org.jgroups.protocols.netty.NettyTP;

import io.netty.channel.EventLoop;

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

   public void completedMessage(Message msg, EventLoop loop) {
      Entry entry = senderTable.get(msg.getSrc());
      if (entry != null) {
         log.trace("%s Marking %s as completed", tp.addr(), msg);
         entry.messageCompleted(msg, loop);
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

   private static final AtomicLongFieldUpdater<Entry> SUBMITTED_MSGS_UPDATER = AtomicLongFieldUpdater.newUpdater(Entry.class, "submitted_msgs");
   private static final AtomicLongFieldUpdater<Entry> QUEUED_MSGS_UPDATER = AtomicLongFieldUpdater.newUpdater(Entry.class, "queued_msgs");

   protected class Entry implements Runnable {
      volatile boolean running = false;
      // This variable is only accessed from the event loop tied with the sender
      protected final ArrayDeque<Message> batch;    // used to queue messages
      protected final Thread eventLoopThread;
      protected final Address      sender;

      protected volatile long               submitted_msgs;
      protected volatile long               queued_msgs;

      // Needs to be volatile as we can read it from a different thread on completion
      protected volatile Message   messageBeingProcessed;
      protected long batchLength = -1;



      protected Entry(Address sender) {
         this.sender=sender;
         batch=new ArrayDeque<>();
         eventLoopThread=Thread.currentThread();
      }


      public Entry reset() {
         submitted_msgs=queued_msgs=0;
         return this;
      }

      protected void messageCompleted(Message msg, EventLoop loop) {
         if (msg != messageBeingProcessed) {
            log.error("%s Inconsistent message completed %s versus processing %s, this is most likely a bug!", tp.addr(), msg, messageBeingProcessed);
         }
         if (loop.inEventLoop()) {
            if (running) {
               log.trace("%s Message %s completed synchronously ", tp.addr(), msg);
               messageBeingProcessed = null;
               return;
            }
         }
         log.trace("%s Message %s completed async, dispatching next message if applicable", tp.addr(), msg);

         // NOTE: we cannot set messageBeingProcessed to null as it was completed asynchronously, because if there is a
         // pending read between our submission below that it enqueues the message

         if (loop.inEventLoop()) {
            run();
         } else {
            loop.execute(this);
         }
      }

      /**
       * Submits the single message and returns whether the message was processed synchronously or not
       * @param msg the message to send up
       * @return whether the message completed synchronously
       */
      protected boolean submitMessage(Message msg) {
         running = true;
         // Following block is just copied from SubmitToThreadPool#SingleMessageHandler instead of allocating a new
         // object and also because the constructor is protected
         {
            Address dest=msg.getDest();
            boolean multicast=dest == null;
            try {
               if(tp.statsEnabled()) {
                  MsgStats msg_stats=tp.getMessageStats();
                  boolean oob=msg.isFlagSet(Message.Flag.OOB);
                  if(oob)
                     msg_stats.incrNumOOBMsgsReceived(1);
                  else
                     msg_stats.incrNumMsgsReceived(1);
                  msg_stats.incrNumBytesReceived(msg.getLength());
               }
               TpHeader hdr=msg.getHeader(tp_id);
               byte[] cname = hdr.getClusterName();
               tp.passMessageUp(msg, cname, true, multicast, true);
            }
            catch(Throwable t) {
               log.error(Util.getMessage("PassUpFailure"), t);
            }

         }
         running = false;
         // Check for the presence of the async header to tell if message may be delayed
         if (!(msg.getHeader(tp.getId()) instanceof NettyAsyncHeader)) {
            messageBeingProcessed = null;
         } else if (messageBeingProcessed != null) {
            log.trace("%s Message %s not completed synchronously, must wait until it is complete later", tp.addr(), msg);
            return false;
         }
         return true;
      }

      public boolean process(Message msg) {
         if (messageBeingProcessed != null) {
            QUEUED_MSGS_UPDATER.incrementAndGet(this);
            batch.add(msg);
            notifyOnWatermarkOverflow(msg.getSrc());
            return false;
         }
         SUBMITTED_MSGS_UPDATER.incrementAndGet(this);
         messageBeingProcessed = msg;
         return submitMessage(msg);
      }

      public boolean process(MessageBatch batch) {
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
            if (!submitMessage(msg)) {
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
         if (batchLength < high_watermark) {
            long newBatchLength = batchLength();
            log.trace("%s Batch size has increased from %s to %s for sender %s", tp.addr(), batchLength, newBatchLength, sender);
            batchLength = newBatchLength;
            if (batchLength > high_watermark) {
               log.trace("%s High watermark met for sender %s, pausing reads", tp.addr(), sender);
               tp.down(new WatermarkOverflowEvent(sender, true));
            }
         }
      }

      // unsynchronized on batch but who cares
      public String toString() {
         return String.format("batch size=%d queued msgs=%d submitted msgs=%d",
               batch.size(), queued_msgs, submitted_msgs);
      }

      protected long batchLength() {
         long size = 0;
         for (Message message : batch) {
            size += message.getLength();
         }
         return size;
      }

      // This code can only be invoked in the event loop for this sender
      @Override
      public void run() {
         long startingLength = batchLength;
         boolean canSendLowWaterMark = high_watermark < startingLength;

         assert messageBeingProcessed != null;
         messageBeingProcessed = null;

         log.trace("%s Batch has %d messages remaining", tp.addr(), batch.size());

         Message msg;
         while ((msg = batch.pollFirst()) != null) {
            messageBeingProcessed = msg;
            if (!submitMessage(msg)) {
               break;
            }
         }
         if (msg != null) {
            long endingLength = batchLength();
            batchLength = endingLength;
            if (canSendLowWaterMark && endingLength < low_watermark) {
               log.trace("%s Low watermark met for %s, resuming reads", tp.addr(), msg.src());
               tp.down(new WatermarkOverflowEvent(msg.src(), false));
            }
         } else {
            assert batchLength == 0;
         }
      }
   }
}
