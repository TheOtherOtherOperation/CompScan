package net.deepstorage.compscan.util;

import java.io.IOException;
import java.io.InputStream;

public interface MultipartByteStream{
   default int read(byte[] buf) throws IOException{
      return read(buf,0,buf.length);
   }
   //Read 0..len bytes and return their number.
   //Don't read past part boundary.
   //Read number may be less than requested, even without the boundary. 
   //possible return values: 0..len
   //if called after isEos() return true, throw exception
   int read(byte[] buf, int off, int len) throws IOException;
   
   //if the last read chunk was the last in the stream
   boolean isEos();
   
   //if the last read chunk was the last in the part
   boolean isPartBoundary();
   
   void close();
   
   default int readFully(byte[] buf, boolean ignoreParts) throws IOException{
      return readFully(buf,0,buf.length,ignoreParts);
   }
   //try to read exactly len bytes
   //@param ignoreParts if  true, don't read past tne part boundary.
   //return values: 0...len
   default int readFully(byte[] buf, int off, int len, boolean ignoreParts) throws IOException{
      int c=0;
      while(c<len){
         int n=read(buf,off+c,len-c);
         c+=n;
         if(!ignoreParts && isPartBoundary()) break;
         if(isEos()) break;
      }
      return c;
   }
   
   public static MultipartByteStream convert(InputStream in){
      return new MultipartByteStream(){
         private boolean eos;
         
         public int read(byte[] buf, int off, int len) throws IOException{
            if(eos) throw new IOException("reading past EOS");
            int c=in.read(buf, off, len);
            if(c<0){
               eos=true;
               c=0;
            }
            return c;
         }
         
         public boolean isEos(){
            return eos;
         }
         
         public boolean isPartBoundary(){
            return eos;
         }
         
         public void close(){
            try{
               in.close();
            }
            catch(Exception e){}
         }
      };
   }
   public static InputStream convert(MultipartByteStream in){
      return new InputStream(){
         byte[] b=new byte[1];
         
         public int read() throws IOException{
            int c=in.readFully(b,true);
            if(c!=1){
               
            }
            return b[0];
         }
         
         public int read(byte[] buf, int off, int len) throws IOException{
            return in.read(buf, off, len);
         }
         
         public void close(){
            in.close();
         }
      };
   }
//   class Adapter implements MultipartByteStream{
//      protected final ByteStream src;
//      private boolean eos;
//      
//      public Adapter(ByteStream src){
//         this.src=src;
//      }
//      
//      public int read(byte[] buf, int off, int len) throws IOException{
//         int c=src.read(buf, off, len);
//         if(c<0) eos=true;
//         return c;
//      }
//      
//      public void close(){
//         src.close();
//      }
//      
//      public boolean isPartBoundary(){
//         return eos;
//      }
//   }
}
