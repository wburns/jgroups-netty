package org.jgroups.util;

import org.jgroups.Event;
import org.jgroups.Message;

// TODO: change this to allow for multiple message completions to avoid going down the stack multiple times
public class MessageCompleteEvent extends Event {
   public MessageCompleteEvent(Message msg) {
      super(Event.USER_DEFINED, msg);
   }
}
