package netty.utils;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

public class ByteBufAsDataInput implements DataInput {
   private final ByteBuf buf;

   public ByteBufAsDataInput(ByteBuf buf) {
      this.buf = buf;
   }

   @Override
   public void readFully(byte[] b) throws IOException {
      if (buf.readableBytes() < b.length) {
         throw new EOFException();
      }
      buf.readBytes(b);
   }

   @Override
   public void readFully(byte[] b, int off, int len) throws IOException {
      if (buf.readableBytes() < len) {
         throw new EOFException();
      }
      buf.readBytes(b, off, len);
   }

   @Override
   public int skipBytes(int n) throws IOException {
      int skipAmount = Math.min(buf.readableBytes(), n);
      buf.skipBytes(skipAmount);
      return skipAmount;
   }

   @Override
   public boolean readBoolean() throws IOException {
      return buf.readBoolean();
   }

   @Override
   public byte readByte() throws IOException {
      if (buf.readableBytes() < 1) {
         throw new EOFException();
      }
      return buf.readByte();
   }

   @Override
   public int readUnsignedByte() throws IOException {
      if (buf.readableBytes() < 1) {
         throw new EOFException();
      }
      return buf.readUnsignedByte();
   }

   @Override
   public short readShort() throws IOException {
      if (buf.readableBytes() < 2) {
         throw new EOFException();
      }
      return buf.readShort();
   }

   @Override
   public int readUnsignedShort() throws IOException {
      if (buf.readableBytes() < 2) {
         throw new EOFException();
      }
      return buf.readUnsignedShort();
   }

   @Override
   public char readChar() throws IOException {
      if (buf.readableBytes() < 2) {
         throw new EOFException();
      }
      return buf.readChar();
   }

   @Override
   public int readInt() throws IOException {
      if (buf.readableBytes() < 4) {
         throw new EOFException();
      }
      return buf.readInt();
   }

   @Override
   public long readLong() throws IOException {
      if (buf.readableBytes() < 8) {
         throw new EOFException();
      }
      return buf.readLong();
   }

   @Override
   public float readFloat() throws IOException {
      if (buf.readableBytes() < 4) {
         throw new EOFException();
      }
      return buf.readFloat();
   }

   @Override
   public double readDouble() throws IOException {
      if (buf.readableBytes() < 8) {
         throw new EOFException();
      }
      return buf.readDouble();
   }

   @Override
   public String readLine() throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   public String readUTF() throws IOException {
      // Assume the writeUTF from
      int utfBytes = readUnsignedShort();
      if (buf.readableBytes() < utfBytes) {
         throw new EOFException();
      }
      int readerPos = buf.readerIndex();
      String string = buf.toString(0, utfBytes, StandardCharsets.UTF_8);
      buf.readerIndex(readerPos + utfBytes);
      return string;
   }
}
