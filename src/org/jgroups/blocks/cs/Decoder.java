package org.jgroups.blocks.cs;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

/**
 * @author baizel
 */
public class Decoder extends MessageToMessageDecoder<ByteBuf> {

    private static final int MIN_LENGTH = Integer.BYTES * 2;
    private ByteBuf tmpBuf;

    public Decoder() {
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf msg, List<Object> out) throws Exception {
//        if (msg.readableBytes() >= MIN_LENGTH) { //wait for  offset and length params to arrive
//            int readerIndex = msg.readerIndex();
//            ByteBuf offsetAndLength = msg.copy(readerIndex, MIN_LENGTH);
//            offsetAndLength.readInt();
//            int length = offsetAndLength.readInt();
//            int dataAvailable = msg.readableBytes() - MIN_LENGTH;
//            assert msg.readerIndex() != offsetAndLength.readerIndex();
//            if (dataAvailable >= length){
//                out.add(ByteBufUtil.getBytes(msg));
//                offsetAndLength.release();
//            }
//        }
        out.add(ByteBufUtil.getBytes(msg));
    }
}
