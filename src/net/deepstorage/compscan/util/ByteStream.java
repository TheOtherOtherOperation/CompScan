package net.deepstorage.compscan.util;

import java.io.IOException;
import java.io.InputStream;

public interface ByteStream{
   //read 0..len bytes and return their actual number;  return -1 if eos already reached
   //actual number may be less than requested, which doesn't mean EOS
   //possible return values: -1, 0..len
   int read(byte[] buf, int off, int len) throws IOException;
   
   default int read(byte[] buf) throws IOException{
      return read(buf,0,buf.length);
   }
   
   
   //try to read exactly len bytes
   //if eos reached, return actually read number
   //returned number being less than requested indicates EOS
   //possible return values: 0..len
   default int readFully(byte[] buf) throws IOException{
      return readFully(buf,0,buf.length);
   }
   default int readFully(byte[] buf, int off, int len) throws IOException{
      int c=0;
      while(c<len){
         int n=read(buf,off+c,len-c);
         if(n<0) break;
         c+=n;
      }
      return c;
   }
   
   void close();
   
   public class Util{
      public static ByteStream convert(InputStream in){
         return new ByteStream(){
            public int read(byte[] buf, int off, int len) throws IOException{
               return in.read(buf, off, len);
            }
            
            public void close(){
               try{
                  in.close();
               }
               catch(Exception e){}
            }
         };
      }
      
      public static InputStream convert(ByteStream in){
         return new InputStream(){
            byte[] b=new byte[1];
            
            public int read() throws IOException{
               int c=in.readFully(b);
               if(c!=1) return c;
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
   }
}