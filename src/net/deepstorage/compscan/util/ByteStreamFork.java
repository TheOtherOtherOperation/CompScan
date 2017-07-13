package net.deepstorage.compscan.util;

import java.io.IOException;

public class ByteStreamFork{
   private final int count;
   private final ByteStream source;
   
   public final ByteStream[] streams;
   
   private final Stream[] kids;
   private boolean eos;
   private int liveCount;
   private int lastStreamIndex;
   
   private final CyclicByteBuffer buffer=new CyclicByteBuffer();
   
   public ByteStreamFork(ByteStream source, int count){
      this.source=source;
      this.count=count;
      kids=new Stream[count];
      for(int i=0;i<count;i++) kids[i]=new Stream(); 
      streams=kids;
      liveCount=count;
      lastStreamIndex=count-1;
   }
   
   public int lastStreamIndex(){
      return lastStreamIndex;
   }
   
   //override to make synchronized version
   protected int read(Stream stream, byte[] dst, int dstOff, int len) throws IOException{
      int size=buffer.size();
      int pos=stream.position;
      int missing=pos+len-size;
      if(missing>0 && !eos){
         int c=buffer.append(source,missing);
         eos= c<missing;
         size+=c;
      }
      int actualBytes=Math.min(len, size-pos);
      if(actualBytes==0 && eos) return -1;
      buffer.read(pos, dst, dstOff, actualBytes);
      if(pos==0){
         stream.position=actualBytes;
         updatePositions();
      }
      else stream.position+=actualBytes;
      return actualBytes;
   }
   
   public void closeAll(){
      for(int i=count;i-->0;) kids[i].closed=true;
      source.close();
   }
   
   private void updatePositions(){
      if(liveCount==0) return;
      int lastStreamIndex=-1;
      int minPos=Integer.MAX_VALUE;
      for(int i=0;i<count;i++){
         Stream s=kids[i];
         if(s.closed) continue;
         int pos=s.position;
         if(pos<minPos){
            minPos=pos;
            lastStreamIndex=i;
         }
      }
      this.lastStreamIndex=lastStreamIndex;
      if(minPos>0) for(int i=0;i<count;i++) kids[i].position-=minPos;
      buffer.skip(minPos);
   }
   
   //override to make synchronized version
   protected void updateClosed(){
      if(--liveCount==0){
         source.close();
         lastStreamIndex=-1;
      }
   }
   
   protected class Stream implements ByteStream{
      int position;
      boolean closed;
      
      public int read(byte[] buf, int off, int len) throws IOException{
         if(closed) throw new IOException("stream is closed");
         return ByteStreamFork.this.read(this, buf, off, len);
      }
      
      public void close(){
         if(closed) return;
         closed=true;
         updateClosed();
      }
   }
   
   public static void main(String[] args) throws Exception{
      byte[] data=new byte[10001];
      for(int i=0;i<data.length;i++) data[i]=(byte)(i+1);
      ByteStream src=ByteStream.Util.convert(new java.io.ByteArrayInputStream(data));
      int[] sizes={11,17,23,90,511,4093};
      int[] totals=new int[sizes.length];
      ByteStreamFork fork=new ByteStreamFork(src,sizes.length);
      CyclicByteBuffer buf=new CyclicByteBuffer(35);
      int si;
      while((si=fork.lastStreamIndex())>=0){
         totals[si]+=testRead(fork.streams[si],buf,sizes[si]);
      }
      System.out.println("totals:");
      for(int i=0;i<totals.length;i++) System.out.println(i+": "+totals[i]);
   }
   private static int testRead(
      ByteStream stream, CyclicByteBuffer buf, int size
   ) throws IOException{
      if(stream==null) return 0;
      buf.clear();
      int c=buf.append(stream,size);
      if(c<size) stream.close();
      return c;
   }
}