package net.deepstorage.compscan.util;

import java.io.IOException;

public class TrottleByteStream implements MultipartByteStream{
   public static final long CHECK_TIME=50;
   public final long CHECK_BYTES;
   
   private final MultipartByteStream src;
   private final double bps;
   
   private long lastCheck;
   private long bytes;
   
   public TrottleByteStream(MultipartByteStream src, double maxBps){
      this.src=src;
      bps=maxBps;
      CHECK_BYTES=(long)(CHECK_TIME*0.001*maxBps);
      lastCheck=System.currentTimeMillis();
   }
   
   public int read(byte[] buf, int off, int len) throws IOException{
      long t=System.currentTimeMillis();
      long dt=t-lastCheck;
      if(dt>CHECK_TIME || bytes>CHECK_BYTES){
         long dt1=(long)(bytes/bps*1000);
         if(dt1>dt){
            try{
               Thread.sleep(dt1-dt);
            }
            catch(Exception e){}
            lastCheck=t;
            bytes=0;
         }
      }
      int c=src.read(buf, off, len);
      if(c>=0) bytes+=c;
      return c;
   }
   
   public void close(){
      src.close();
   }
   
   public boolean isEos(){
      return src.isEos();
   }
   
   public boolean isPartBoundary(){
      return src.isPartBoundary();
   }
}
