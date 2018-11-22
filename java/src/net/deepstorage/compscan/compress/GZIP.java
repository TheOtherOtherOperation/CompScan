package net.deepstorage.compscan.compress;

import java.io.*;
import java.util.zip.*;
import net.deepstorage.compscan.*;
import net.deepstorage.compscan.util.Util;
import net.deepstorage.compscan.Compressor.BufferLengthException;

public class GZIP implements CompressionInterface{
   public final static int BEST_SPEED=Deflater.BEST_SPEED;
   public final static int BEST_COMPRESSION=Deflater.BEST_COMPRESSION;
   public final static int DEFAULT_COMPRESSION=6;//Deflater.DEFAULT_COMPRESSION;
   
   private final ByteArrayOutputStream buffer=new ByteArrayOutputStream();
   private final GZIPOutputStream gzip;
   private Deflater deflater;
   private final int level;
   
   public GZIP(){
      this(6);
   }
   
   public GZIP(int level){
      if(level<1 || level>9) throw new IllegalArgumentException(""+level);
      try{
         gzip=new GZIPOutputStream(buffer){{
            deflater=def;
         }};
      }
      catch(IOException e){ //never happen anyway
         throw new Error(e);
      }
      deflater.setLevel(this.level=level);
   }
   
   public void setOptions(String s){
      try{
         int lvl=Integer.parseInt(s);
         if(lvl<BEST_SPEED || lvl>BEST_COMPRESSION) throw new IllegalArgumentException(
            "Wrong level: "+lvl+", expect "+BEST_SPEED+" to "+BEST_COMPRESSION
         );
         deflater.setLevel(lvl);
      }
      catch(NumberFormatException e){
         throw new IllegalArgumentException(
            "Wrong option to LZ4 compressor: "+s+", e.lengthxpect number 0 to 17"
         );
      }
   }
   
   //same instance is reused, synchronize
   public synchronized byte[] compress(byte[] data, int blockSize) throws BufferLengthException{
      buffer.reset();
      deflater.reset();
      try{
         gzip.write(data,0,data.length);
         gzip.finish();
      }
      catch(IOException e){ //never happen
         throw new Error(e);
      }
      return buffer.toByteArray();
   }
   
   public String toString(){
      return "GZIP:"+level;
   }
   
   public static void main(String[] args) throws Exception{
      GZIP gzip=new GZIP();
      byte[] data=new byte[2048];
      SpeedTest.fillqr(data, 1);
      System.out.println(gzip.compress(data,-1).length);
      System.out.println(gzip.compress(data,-1).length);
      System.out.println(gzip.compress(data,-1).length);
//      SpeedTest.test(new GZIP(BEST_SPEED));
//      SpeedTest.test(new GZIP(DEFAULT_COMPRESSION));
//      SpeedTest.test(new GZIP(BEST_COMPRESSION));
   }
}