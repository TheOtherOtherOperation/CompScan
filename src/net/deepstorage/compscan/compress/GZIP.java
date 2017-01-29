package net.deepstorage.compscan.compress;

import java.io.*;
import java.util.zip.*;
import net.deepstorage.compscan.*;
import net.deepstorage.compscan.Compressor.BufferLengthException;

public class GZIP implements CompressionInterface{
   public final static int BEST_SPEED=Deflater.BEST_SPEED;
   public final static int BEST_COMPRESSION=Deflater.BEST_COMPRESSION;
   public final static int DEFAULT_COMPRESSION=Deflater.DEFAULT_COMPRESSION;
   
   private final ByteArrayOutputStream buffer=new ByteArrayOutputStream();
   private final GZIPOutputStream gzip;
   private Deflater deflater;
   
   public GZIP(int level){
      try{
         gzip=new GZIPOutputStream(buffer){{
            deflater=def;
         }};
      }
      catch(IOException e){ //never happen anyway
         throw new Error(e);
      }
      deflater.setLevel(level);
   }
   
   public byte[] compress(byte[] data, int blockSize) throws BufferLengthException{
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
   
   public static void main(String[] args) throws Exception{
      SpeedTest.test(new GZIP(BEST_SPEED));
      SpeedTest.test(new GZIP(DEFAULT_COMPRESSION));
      SpeedTest.test(new GZIP(BEST_COMPRESSION));
   }
}