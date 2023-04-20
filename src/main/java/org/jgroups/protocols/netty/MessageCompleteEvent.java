package org.jgroups.protocols.netty;

import org.jgroups.Event;
import org.jgroups.Message;

public class MessageCompleteEvent extends Event {
   public MessageCompleteEvent(Message msg) {
      super(Event.USER_DEFINED, msg);
   }
}
