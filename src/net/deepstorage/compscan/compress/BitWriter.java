package net.deepstorage.compscan.compress;

import java.util.Arrays;

public class BitWriter{
   private int cache;
   private int bitsInCache=0;
   
   private int bufSize;
   private byte[] buffer=new byte[100];

   //don't write over 24 bits at once
   public void write(int bits, int n){
      cache|=bits<<bitsInCache;
      bitsInCache+=n;
      while(bitsInCache>8){
         write(cache&0xff);
         cache>>=8;
         bitsInCache-=8;
      }
   }
   
   public void flush(){
      while(bitsInCache>0){
         write(cache&0xff);
         cache>>=8;
         bitsInCache-=8;
      }
   }
   
   private final void write(int b){
      if(bufSize>=buffer.length){
         byte[] newbuf=new byte[buffer.length*3/2+100];
         System.arraycopy(buffer,0,newbuf,0,bufSize);
         buffer=newbuf;
      }
      buffer[bufSize++]=(byte)b;
   }
   
   public void reset(){
      bitsInCache=0;
      bufSize=0;
   }
   
   public byte[] toArray(){
      return Arrays.copyOf(buffer,bufSize);
   }
   
   public static void main(String[] args){
      BitWriter bw=new BitWriter();
      int M=1000;
      Runnable r=new Runnable(){ public void run(){
         bw.reset();
         try{
            for(int i=M;i-->0;) bw.write(i,11);
         }
         catch(Exception ignore){}
      }};
      double cps=SpeedTest.run(r);
      System.out.println("BitWriter speed: "+(cps*M)+" B/s");
   }
}