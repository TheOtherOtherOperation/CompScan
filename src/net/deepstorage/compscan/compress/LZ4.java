package net.deepstorage.compscan.compress;

import net.deepstorage.compscan.CompressionInterface;
import net.jpountz.lz4.*;

public class LZ4 implements CompressionInterface{
   public static final int LVL_FAST=0;
   public static final int LVL_NORMAL=9;
   public static final int LVL_MAX=17;
   
   private final static LZ4Factory factory=LZ4Factory.fastestInstance();
   
   private final LZ4Compressor compressor;
   
   public LZ4(){
      this(LVL_NORMAL);
   }
   
   public LZ4(int level){
      if(level<0) throw new IllegalArgumentException();
      if(level>LVL_MAX) throw new IllegalArgumentException();
      if(level==0) compressor=factory.fastCompressor();
      else compressor=factory.highCompressor(level);
   }
   
   public byte[] compress(byte[] data, int blockSize){
      return compressor.compress(data);
   }
   
   public static void main(String[] args) throws Exception{
      //LZ4 lz4=new LZ4(LZ4.LVL_FAST);
      LZ4 lz4=new LZ4();
      byte[] data=new byte[2000];
      for(int i=1;i<data.length;i++) data[i]=(byte)(i+data[i-1]*37);
      
      Runnable r=new Runnable(){ public void run(){
         lz4.compress(data,data.length);
      }};
      System.out.println("LZW speed: "+(SpeedTest.run(r)*data.length)+" B/s");
      
      byte[] cdata=lz4.compress(data, data.length);
      System.out.println("compressed size: "+cdata.length);
//      System.out.println("currentCode: "+lzw.currentCode);
//      System.out.println("threshold: "+lzw.threshold);
//      System.out.println("width: "+lzw.width);
////      lzw.rootNode.print("");
//      
//      ByteArrayOutputStream buf=new ByteArrayOutputStream();
//      java.util.zip.GZIPOutputStream gzip=new java.util.zip.GZIPOutputStream(buf);
//      gzip.write(data);
//      gzip.close();
//      System.out.println("zipped: "+buf.size());
//      
////      FileOutputStream file=new FileOutputStream("data.bin");
////      file.write(data);
////      file.close();
   }
}