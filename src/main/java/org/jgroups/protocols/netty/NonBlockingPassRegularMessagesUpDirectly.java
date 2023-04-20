package org.jgroups.protocols.netty;

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
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.MaxOneThreadPerSender;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.SubmitToThreadPool;

import io.netty.channel.EventLoop;

/**
 * This class is very similar to {@link MaxOneThreadPerSender} and in fact many of the code is copied from there.
 * The big difference is that it will never block the invoking thread.
 * The general idea is that non oob messages will be processed in order, as usual, but we rely upon external notifications
 * to tell us that the message was completed via
 */
public class NonBlockingPassRegularMessagesUpDirectly extends SubmitToThreadPool {
   protected final ConcurrentMap<Address, Entry> mcasts = new ConcurrentHashMap<>();
   protected final ConcurrentMap<Address, Entry> ucasts = new ConcurrentHashMap<>();

   public void viewChange(List<Address> members) {
      mcasts.keySet().retainAll(members);
      ucasts.keySet().retainAll(members);
   }

   public void destroy() {
      mcasts.clear();
      ucasts.clear();
   }

   @Override
   public void reset() {
      mcasts.values().forEach(Entry::reset);
      ucasts.values().forEach(Entry::reset);
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
      return String.format("\nmcasts:\n%s\nucasts:\n%s", mapToString(mcasts), mapToString(ucasts));
   }

   static String mapToString(ConcurrentMap<Address, Entry> map) {
      return map.values().stream()
            .map(Object::toString)
            .collect(Collectors.joining("\n", "", ""));
   }

   public void completedMessage(Message msg, EventLoop loop) {
      ConcurrentMap<Address, Entry> map = msg.getDest() == null ? mcasts : ucasts;
      Entry entry = map.get(msg.getSrc());
      if (entry != null) {
         log.trace("Marking %s as completed", msg);
         entry.messageCompleted(msg, loop);
      } else {
         log.debug("Message %s was marked as completed, but was not present in MessageTable, most likely concurrent stop", msg);
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
      boolean multicast = batch.getDest() == null;
      ConcurrentMap<Address, Entry> map = multicast ? mcasts : ucasts;
      Entry entry = map.computeIfAbsent(batch.sender(), Entry::new);
      return entry.process(batch);
   }

   @Override
   public boolean process(Message msg, boolean oob) {
      if (oob) {
         return super.process(msg, true);
      }
      boolean multicast = msg.getDest() == null;
      ConcurrentMap<Address, Entry> map = multicast ? mcasts : ucasts;
      Entry entry = map.computeIfAbsent(msg.getSrc(), Entry::new);
      return entry.process(msg);
   }

   private static final AtomicLongFieldUpdater<Entry> SUBMITTED_MSGS_UPDATER = AtomicLongFieldUpdater.newUpdater(Entry.class, "submitted_msgs");
   private static final AtomicLongFieldUpdater<Entry> QUEUED_MSGS_UPDATER = AtomicLongFieldUpdater.newUpdater(Entry.class, "queued_msgs");

   private static final short GMS_ID = ClassConfigurator.getProtocolId(GMS.class);
   private static final short NAKACK_ID = ClassConfigurator.getProtocolId(NAKACK2.class);

   protected class Entry implements Runnable {
      volatile boolean running = false;
      // This variable is only accessed from the event loop tied with the sender
      protected final ArrayDeque<Message> batch;    // used to queue messages
      protected final Address      sender;

      protected volatile long               submitted_msgs;
      protected volatile long               queued_msgs;

      // Needs to be volatile as we can read it from a different thread on completion
      protected volatile Message   messageBeingProcessed;
      protected long batchLength = -1;



      protected Entry(Address sender) {
         this.sender=sender;
         batch=new ArrayDeque<>();
      }


      public Entry reset() {
         submitted_msgs=queued_msgs=0;
         return this;
      }

      protected void messageCompleted(Message msg, EventLoop loop) {
         if (msg != messageBeingProcessed) {
            log.error("Inconsistent message completed %s versus processing %s, this is most likely a bug!", msg, messageBeingProcessed);
         } else {
            log.trace("Message %s completed, dispatching next message if applicable", msg);
         }
         messageBeingProcessed = null;
         // If we aren't running that means we were an asynchronously processed message and we need to make sure
         // to continue any additional tasks on the netty event loop
         if (!running) {
            if (loop.inEventLoop()) {
               run();
            } else {
               loop.execute(this);
            }
         }
      }

      /**
       * Submits the single message and returns whether the message was processed synchronously or not
       * @param msg the message to send up
       * @return whether the message completed synchronously
       */
      protected boolean submitMessage(Message msg) {
         SingleMessageHandler smh=new SingleMessageHandler(msg);
         running = true;
         smh.run();
         running = false;
         if (msg.getHeader(GMS_ID) != null || msg.getType() == Message.EMPTY_MSG || msg.getHeader(NAKACK_ID) != null) {
            // Assume GMS processed synchronously
            messageBeingProcessed = null;
         }
         if (messageBeingProcessed != null) {
            log.trace("Message %s not completed synchronously, must wait until it is complete later", msg);
            return false;
         }
         return true;
      }

      protected boolean process(Message msg) {
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

      protected boolean process(MessageBatch batch) {
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

      private void notifyOnWatermarkOverflow(Address addr) {
         if (batchLength < high_watermark) {
            batchLength = batchLength();
            if (batchLength > high_watermark) {
               log.trace("High watermark met for %s, pausing reads", addr);
               tp.down(new WatermarkOverflowEvent(addr, true));
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

      @Override
      public void run() {
         long startingLength = batchLength;
         boolean canSendLowWaterMark = high_watermark < startingLength;

         assert messageBeingProcessed == null;

         log.trace("Batch has %d messages renaming", batch.size());

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
               log.trace("Low watermark met for %s, resuming reads", msg.src());
               tp.down(new WatermarkOverflowEvent(msg.src(), false));
            }
         } else {
            assert batchLength == 0;
         }
      }
   }
}
