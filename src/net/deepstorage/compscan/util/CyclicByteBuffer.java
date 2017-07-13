package net.deepstorage.compscan.util;

import java.io.IOException;

public class CyclicByteBuffer{
   private byte[] buffer;
   private int start, end; //end==start - empty
   
   public CyclicByteBuffer(){
      this(128);
   }
   
   public CyclicByteBuffer(int initialCapacity){
      buffer=new byte[initialCapacity];
      start=end=0;
   }
   
   public int read(int off, byte[] dst){
      return read(off, dst, 0, dst.length);
   }
   
   public int read(int off, byte[] dst, int dstOff, int len){
      if(off>size()) throw new IllegalArgumentException(off+" > "+size());
      len=Math.min(len,size()-off);
      int flatSpace=buffer.length-start;
      if(off+len<=flatSpace){
         //len already trimmed to end
         System.arraycopy(buffer, start+off, dst, dstOff, len);
         return len;
      }
      int chunk=0;
      if(off<flatSpace){
         chunk=flatSpace-off;
         System.arraycopy(buffer, start+off, dst, dstOff, chunk);
         off=0;
         len-=chunk;
         dstOff+=chunk;
      }
      else off-=flatSpace;
      System.arraycopy(buffer, off, dst, dstOff, len);
      return chunk+len;
   }
   
   public void write(int off, byte[] src){
      write(off, src, 0, src.length);
   }
   
   public void write(int off, byte[] src, int srcOff, int len){
      if(off>size()) throw new IllegalArgumentException(off+" > "+size());
      ensureCapacity(off+len);
      int flatSpace=buffer.length-start;
      if(off+len<=flatSpace){
         System.arraycopy(src,srcOff,buffer,start+off,len);
         end= end<start? end: Math.max(end,start+off+len);
         return;
      }
      if(off<flatSpace){
         int chunk=flatSpace-off;
         System.arraycopy(src,srcOff,buffer,start+off,chunk);
         off=0;
         srcOff+=chunk;
         len-=chunk;
      }
      else off-=flatSpace;
      System.arraycopy(src,srcOff,buffer,off,len);
      end= end>=start? len: Math.max(end,off+len);
   }
   
   //same semantics as ByteStream.readFully
   public int write(int off, ByteStream src, int len) throws IOException{
      if(off>size()) throw new IllegalArgumentException(off+" > "+size());
      ensureCapacity(off+len);
      int flatSpace=buffer.length-start;
      if(len+off<=flatSpace){
         int c=src.readFully(buffer,start+off,len);
         end= end<start? end: Math.max(end,start+off+c);
         return c;
      }
      int chunk=0;
      if(off<flatSpace){
         chunk=flatSpace-off;
         int c=src.readFully(buffer,start+off,chunk);
         if(c<chunk){
            end= end<start? end: Math.max(end,start+off+c);
            return c;
         }
         off=0;
         len-=chunk;
      }
      else off-=flatSpace;
      int c=src.readFully(buffer,off,len);
      end= end>=start? c: Math.max(off+c,end);
      return chunk+c;
   }
   
   public void append(byte[] src){
      write(size(), src, 0, src.length);
   }
   
   public void append(byte[] src, int srcOff, int len){
      write(size(), src, srcOff, len);
   }
   
   public int append(ByteStream src, int len) throws IOException{
      return write(size(), src, len);
   }
   
   public void skip(int n){
      if(n>size()) throw new IllegalArgumentException(n+" > "+size());
      start+=n;
      if(start>buffer.length) start-=buffer.length;
   }
   
   public int size(){
      return end>=start? end-start: buffer.length-start+end;
   }
   
   public void clear(){
      skip(size());
   }
   
   private void ensureCapacity(int n){
      if(buffer.length-1>=n) return;
      int newLen=n*3/2+31;
      byte[] newBuffer=new byte[newLen];
      if(end>=start){
         System.arraycopy(buffer, start, newBuffer, 0, end-start);
         end-=start;
         start=0;
      }
      else{
         int chunk=buffer.length-start;
         System.arraycopy(buffer, start, newBuffer, 0, chunk);
         System.arraycopy(buffer, 0, newBuffer, chunk, end);
         end+=chunk;
         start=0;
      }
      buffer=newBuffer;
   }
   
   public String toString(){
      StringBuilder sb=new StringBuilder("[");
      int size=size();
      for(int c=0; c<size; c++){
         int i=(start+c)%buffer.length;
         if(sb.length()>1) sb.append(", ");
         sb.append(buffer[i]&0xff);
      }
      sb.append(']');
      return sb.toString();
   }
   
   public String toString(boolean content, boolean buffer){
      String s="CyclicByteBuffer("+this.buffer.length+","+start+","+end+","+size();
      if(content) s+=",c:"+this;
      if(buffer) s+=",b:"+Util.toString(this.buffer);
      return s+")";
   }
   
   public static void main(String[] args) throws Exception{
      CyclicByteBuffer buf=new CyclicByteBuffer(20);
      buf.write(0, new byte[]{1,2,3,4,5,6});
      System.out.println(buf);
      buf.write(6, new byte[]{7,8,9,1,2,3});
      System.out.println(buf);
      buf.write(12, new byte[]{4,5,6,7,8,9});
      System.out.println(buf);
      buf.write(18, new byte[]{1,2,3,4,5,6});
      System.out.println(buf);
      buf.write(24, new byte[]{7,8,9,1,2,3});
      System.out.println(buf);
      buf.write(30, new byte[]{4,5,6,7,8,9});
      System.out.println(buf.toString(true,false));
      buf.skip(10);
      System.out.println(buf.toString(true,false));
      buf.skip(10);
      System.out.println(buf.toString(true,false));
      buf.skip(10);
      System.out.println(buf.toString(true,false));
      buf.append(new byte[]{4,5,6,7,8,9});
      System.out.println(buf.toString(true,false));
      buf.append(new byte[54]);
      System.out.println(buf.toString(true,false));
      buf.append(new byte[]{1});
      System.out.println(buf.toString(true,false));
      buf.skip(67);
      System.out.println(buf.toString(true,false));//must be CyclicByteBuffer(131,67,67,0,[])
      
      //CyclicByteBuffer buf;
      buf=new CyclicByteBuffer(25);//65);
      int rep=2;//3;
      
      System.out.println("=======");
      for(int i=rep;i-->0;) buf.append(ByteStream.Util.convert(
         new java.io.ByteArrayInputStream(new byte[]{0,1,2,3,4,5,6,7,8,9})
      ), 10);
      System.out.println(buf.toString(true,true));
      buf.skip(buf.size());
      for(int i=rep;i-->0;) buf.append(ByteStream.Util.convert(
         new java.io.ByteArrayInputStream(new byte[]{10,11,12,13,14,15,16,17,18,19})
      ), 10);
      System.out.println(buf.toString(true,true));
      buf.skip(buf.size());
      for(int i=rep;i-->0;) buf.append(ByteStream.Util.convert(
         new java.io.ByteArrayInputStream(new byte[]{20,21,22,23,24,25,26,27,28,29})
      ), 10);
      System.out.println(buf);
      
      System.out.println("=======");
      //CyclicByteBuffer buf;
      buf=new CyclicByteBuffer(25);//65);
      buf.start=0;
      buf.end=20;
      System.out.println(buf.toString(true,true));
      buf.skip(buf.size());
      System.out.println(buf.toString(true,true));
      buf.append(new byte[]{1,1,1,1,1,1,1,1,1,1});
//      buf.append(ByteStream.Util.convert(
//         new java.io.ByteArrayInputStream(new byte[]{1,1,1,1,1,1,1,1,1,1})
//      ), 10);
      System.out.println(buf.toString(true,true));
      buf.append(new byte[]{2,2,2,2,2,2,2,2,2,2});
//      buf.append(ByteStream.Util.convert(
//         new java.io.ByteArrayInputStream(new byte[]{2,2,2,2,2,2,2,2,2,2})
//      ), 10);
      System.out.println(buf.toString(true,true));
   }
}
