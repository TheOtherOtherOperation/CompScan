package net.deepstorage.compscan.compress;

import net.deepstorage.compscan.CompressionInterface;
import net.jpountz.lz4.*;

public class LZ4 implements CompressionInterface{
   public static final int LVL_FAST=0;
   public static final int LVL_NORMAL=9;
   public static final int LVL_MAX=17;
   
   private final static LZ4Factory factory=LZ4Factory.fastestInstance();
   
   private int level=-1;
   private LZ4Compressor compressor;
   
   public LZ4(){
      this(LVL_NORMAL);
   }
   
   public LZ4(int level){
      setLevel(level);
   }
   
   public void setOptions(String s){
      try{
         setLevel(Integer.parseInt(s));
      }
      catch(NumberFormatException e){
         throw new IllegalArgumentException(
            "Wrong option to LZ4 compressor: "+s+", expect number 0 to 17"
         );
      }
   }
   
   private void setLevel(int level){
      if(level<0 || level>LVL_MAX) throw new IllegalArgumentException("Wrong level: "+level+", expect 0 to "+LVL_MAX);
      if(level!=this.level) compressor=null;
      this.level=level;
   }
   
   private LZ4Compressor getCompressor(){
      if(compressor!=null) return compressor;
      compressor= level==0? factory.fastCompressor(): factory.highCompressor(level);
      return compressor;
   }
   
   public byte[] compress(byte[] data, int blockSize){
      return getCompressor().compress(data);
   }
   
   public String toString(){
      return "LZ4:"+level;
   }
   
   public static void main(String[] args) throws Exception{
      //LZ4 lz4=new LZ4(LZ4.LVL_FAST);
      LZ4 lz4=new LZ4(0);
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