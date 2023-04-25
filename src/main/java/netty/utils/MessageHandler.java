package netty.utils;

import java.io.DataInput;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class MessageHandler extends ByteToMessageDecoder {
   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf msgbuf, List<Object> out) throws Exception {
      int startingPos = msgbuf.readerIndex();
      if (msgbuf.readableBytes() < 4) {
         return;
      }
      int totalLength = msgbuf.readInt();
      if (msgbuf.readableBytes() < totalLength) {
         msgbuf.readerIndex(startingPos);
         return;
      }
      // Just ignore the address length as it can parse it itself
      msgbuf.readerIndex(msgbuf.readerIndex() + 4);
      out.add(new ByteBufAsDataInput(msgbuf));
   }
}
