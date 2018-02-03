package net.deepstorage.compscan.compress;

import net.deepstorage.compscan.CompressionInterface;
import java.io.*;

public class SpeedTest {
   static CompressionInterface[] compressors=new CompressionInterface[]{
//      new OldLZW(), 
//      new LZW(),
      new LZ4(0),
      new LZ4(1),
      new LZ4(6),
      new LZ4(12),
      new GZIP(1),
      new GZIP(6),
      new GZIP(9),
   };
   
   static int BLOCK_SIZE=10000;
   
   public static void test(CompressionInterface compressor) throws Exception{
      System.out.println("testing "+compressor);
      
      byte[] src=new byte[BLOCK_SIZE];
      int seed=fillqr(src,37);
      
      double cps=run(new Runnable(){public void run(){
         try{
            compressor.compress(src,src.length);
         }
         catch(Exception ignore){}
      }});
      
      System.out.println(" troughput: "+(BLOCK_SIZE*cps)+" bytes/s");
   }
   
   //quasy-random data
   static int fillqr(byte[] data, int seed){
      for(int i=0;i<data.length;i++){
         data[i]=(byte)seed;
         seed^=seed<<13;
         seed^=seed>>17;
         seed^=seed<<5;
      }
      return seed;
   }
   
   //return calls per sec
   public static double run(Runnable r){
      int N=1;
      long dt=0;
      while(dt<3000){
         N*=2;
         long t0=System.currentTimeMillis();
         for(int i=N;i-->0;) r.run();
         dt=System.currentTimeMillis()-t0;
      }
      return N*1000.0/dt;
   }
   
   public static void main(String[] args) throws Exception{
      for(CompressionInterface comp:compressors){
         test(comp);
      }
   }
}