package net.deepstorage.compscan.util;

import java.io.IOException;

public interface MultipartByteStream extends ByteStream{
   public boolean isPartBoundary();
   
   class Adapter implements MultipartByteStream{
      protected final ByteStream src;
      private boolean eos;
      
      public Adapter(ByteStream src){
         this.src=src;
      }
      
      public int read(byte[] buf, int off, int len) throws IOException{
         int c=src.read(buf, off, len);
         if(c<0) eos=true;
         return c;
      }
      
      public void close(){
         src.close();
      }
      
      public boolean isPartBoundary(){
         return eos;
      }
   }
}
