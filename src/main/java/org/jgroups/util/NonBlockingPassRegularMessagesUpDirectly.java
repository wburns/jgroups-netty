package org.jgroups.util;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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

   private static final int DEFAULT_LOW_WATER_MARK = 4 * 1024;
   private static final int DEFAULT_HIGH_WATER_MARK = 12 * 1024;

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

   // Prevent allocating lambda per OOB message
   protected final Function<Address, Entry> NEW_ENTRY_FUNCTION = s -> new Entry(s, tp.getClusterNameAscii());

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
      throw new UnsupportedOperationException("Loopback is not supported with this TP");
   }

   @Override
   public boolean process(MessageBatch batch, boolean oob) {
      if (oob) {
         return super.process(batch, true);
      }
      Entry entry = senderTable.computeIfAbsent(batch.sender(), NEW_ENTRY_FUNCTION);
      return entry.process(batch);
   }

   @Override
   public boolean process(Message msg, boolean oob) {
      if (oob) {
         return super.process(msg, true);
      }
      Entry entry = senderTable.computeIfAbsent(msg.getSrc(), NEW_ENTRY_FUNCTION);
      return entry.process(msg);
   }

   protected class Entry implements Runnable {
      // This variable is only accessed from the event loop tied with the sender
      protected final MessageBatch batch;    // used to queue messages
      protected final MessageBatch sendingBatch; // used to send batch up
      protected long               submitted_msgs;
      protected long               submitted_batches;
      protected long               queued_msgs;
      protected long               queued_batches;

      // Needs to be volatile as messageCompleted can be invoked from a different thread, all else are on event loop
      volatile boolean running = false;
      // Needs to be volatile as messageCompleted can be invoked from a different thread, all else are on event loop
      protected int outstandingMessageBytes = 0;
      // This is used by async completions so that only the first has to process the outstanding update as concurrent
      // updates will just add to this instead
      protected final AtomicInteger asyncByteUpdate = new AtomicInteger();

      protected Entry(Address sender, AsciiString cluster_name) {
         batch=new MessageBatch(16).dest(tp.getAddress()).sender(sender).clusterName(cluster_name).multicast(sender== tp.addr());
         sendingBatch=new MessageBatch(16).dest(tp.getAddress()).sender(sender).clusterName(cluster_name).multicast(sender== tp.addr());
      }


      public Entry reset() {
         submitted_msgs=submitted_batches=queued_msgs=queued_batches=0;
         return this;
      }

      protected void messageCompleted(Message msg, EventLoop loop) {
         if (loop.inEventLoop()) {
            log.trace("%s Message %s completed synchronously ", tp.addr(), msg);
            notifyOnWatermarkUnderflow(msg.src(), msg.getLength());
            return;
         }

         int pendingSize = asyncByteUpdate.getAndAdd(msg.getLength());
         if (pendingSize == 0) {
            log.trace("%s Scheduling message %s complete checks to fire on event loop ", tp.addr(), msg);
            loop.submit(this);
         } else {
            log.trace("%s Added length to pending event loop message complete notification, currently %d", tp.addr(), pendingSize);
         }
      }

      /**
       * Submits the single message and returns whether the message was processed synchronously or not
       * @param msg the message to send up
       * @return whether the message completed synchronously
       */
      protected boolean submitMessage(Message msg) {
         boolean multicast=msg.getDest() == null;
         try {
            if(tp.statsEnabled()) {
               messageStats(msg);
            }
            TpHeader hdr=msg.getHeader(tp_id);
            running = true;
            tp.passMessageUp(msg, hdr.getClusterName(), true, multicast, true);
            running = false;
         }
         catch(Throwable t) {
            log.error(Util.getMessage("PassUpFailure"), t);
         }

         running = false;
         // Check for the presence of the async header to tell if message may be delayed
         if (!(msg.getHeader(tp.getId()) instanceof NettyAsyncHeader)) {
            outstandingMessageBytes = 0;
         } else if (outstandingMessageBytes > 0) {
            log.trace("%s Message %s not completed synchronously, must wait until it is complete later", tp.addr(), msg);
            return false;
         }
         return true;
      }

      private void messageStats(Message msg) {
         MsgStats msg_stats=tp.getMessageStats();
         boolean oob=msg.isFlagSet(Message.Flag.OOB);
         if(oob)
            msg_stats.incrNumOOBMsgsReceived(1);
         else
            msg_stats.incrNumMsgsReceived(1);
         msg_stats.incrNumBytesReceived(msg.getLength());
      }

      protected boolean submitBatch(MessageBatch batch, int batchLength) {
         if(tp.statsEnabled()) {
            batchStats(batch, batchLength);
         }
         running = true;
         tp.passBatchUp(batch, false, true);
         // First check if batch was fully consumed with notified messages
         if (outstandingMessageBytes != 0) {
            // Decrement any messages that don't have the header as they are assumed completed
            for (Message msg : batch) {
               if (!(msg.getHeader(tp.getId()) instanceof NettyAsyncHeader)) {
                  outstandingMessageBytes -= msg.getLength();
               }
            }
            if (outstandingMessageBytes != 0) {
               batch.clear();
               return false;
            }
         }
         batch.clear();
         return true;
      }

      private void batchStats(MessageBatch batch, int batchLength) {
         int batch_size=batch.size();
         MsgStats msg_stats=tp.getMessageStats();
         boolean oob=batch.getMode() == MessageBatch.Mode.OOB;
         if(oob)
            msg_stats.incrNumOOBMsgsReceived(batch_size);
         else
            msg_stats.incrNumMsgsReceived(batch_size);
         msg_stats.incrNumBatchesReceived(1);
         msg_stats.incrNumBytesReceived(batchLength);
         tp.avgBatchSize().add(batch_size);
      }

      public boolean process(Message msg) {
         if (outstandingMessageBytes > 0) {
            queued_msgs++;
            batch.add(msg);
            notifyOnWatermarkOverflow(msg.getSrc(), msg.getLength());
            return false;
         }
         submitted_msgs++;
         outstandingMessageBytes = msg.getLength();
         return submitMessage(msg);
      }

      public boolean process(MessageBatch batch) {
         int batchLength = batch.length();
         if (outstandingMessageBytes > 0) {
            queued_batches++;
            this.batch.transferFrom(batch, false);
            notifyOnWatermarkOverflow(batch.sender(), batchLength);
            return false;
         }
         submitted_batches++;
         outstandingMessageBytes = batchLength;
         if (submitBatch(batch, batchLength)) {
            return true;
         }
         if (outstandingMessageBytes > high_watermark) {
            log.trace("%s High watermark met for sender %s, pausing reads", tp.addr(), batch.sender);
            tp.down(new WatermarkOverflowEvent(batch.sender, true));
         }
         return false;
      }

      private void notifyOnWatermarkOverflow(Address sender, int length) {
         int prev = outstandingMessageBytes;
         outstandingMessageBytes += length;
         log.trace("%s Batch size has increased from %s to %s for sender %s", tp.addr(), prev, outstandingMessageBytes, sender);
         if (prev < high_watermark && outstandingMessageBytes > high_watermark) {
            log.trace("%s High watermark met for sender %s, pausing reads", tp.addr(), sender);
            tp.down(new WatermarkOverflowEvent(sender, true));
         }
      }

      private void notifyOnWatermarkUnderflow(Address sender, int length) {
         int prev = outstandingMessageBytes;
         outstandingMessageBytes -= length;
         log.trace("%s Batch size has decreased from %s to %s for sender %s", tp.addr(), prev, outstandingMessageBytes, sender);
         if (prev > high_watermark && outstandingMessageBytes < high_watermark) {
            log.trace("%s Low watermark met for sender %s, resuming reads", tp.addr(), sender);
            tp.down(new WatermarkOverflowEvent(sender, false));
         }

         int awaitingCommandBytes = outstandingMessageBytes - batch.length();
         if (awaitingCommandBytes == 0) {
            sendPendingBatch();
         } else {
            log.trace("%s Still awaiting %d bytes of downstream commands to start next batch", tp.addr(), awaitingCommandBytes);
         }
      }

      // unsynchronized on batch but who cares
      public String toString() {
         return String.format("batch size=%d queued msgs=%d submitted msgs=%d",
               batch.size(), queued_msgs, submitted_msgs);
      }

      private void sendPendingBatch() {
         if (batch.isEmpty()) {
            log.trace("%s Batch to %s as empty, ignoring", tp.addr(), tp.addr());
            return;
         }
         sendingBatch.transferFrom(batch, true);

         log.trace("%s Batch has %d messages", tp.addr(), sendingBatch.size());
         int batchLength = sendingBatch.length();
         outstandingMessageBytes = batchLength;
         submitBatch(sendingBatch, batchLength);
         sendingBatch.clear();
         if (outstandingMessageBytes > high_watermark) {
            log.trace("%s High watermark met for sender %s after processing low water mark, pausing reads", tp.addr(), sendingBatch.sender);
            tp.down(new WatermarkOverflowEvent(sendingBatch.sender, true));
         }
      }

      // This code can only be invoked in the event loop for this sender
      @Override
      public void run() {
         int bytesToDecrement = asyncByteUpdate.getAndSet(0);
         notifyOnWatermarkUnderflow(sendingBatch.sender, bytesToDecrement);
      }
   }
}
