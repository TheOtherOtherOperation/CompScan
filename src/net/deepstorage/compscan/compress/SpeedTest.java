package net.deepstorage.compscan.compress;

import net.deepstorage.compscan.CompressionInterface;

public class SpeedTest {
   static CompressionInterface[] compressors=new CompressionInterface[]{
//      new OldLZW(), 
      new LZW(),
   };
   
   static int BLOCK_SIZE=10000;
   
   public static void test(CompressionInterface compressor) throws Exception{
      System.out.println("testing "+compressor.getClass().getSimpleName());
      byte[] src=new byte[BLOCK_SIZE];
      for(int i=1;i<src.length;i++) src[i]=(byte)(i+src[i]*37);
      
      double cps=run(new Runnable(){public void run(){
         try{
            compressor.compress(src,BLOCK_SIZE);
         }
         catch(Exception ignore){}
      }});
      
      System.out.println(" troughput: "+(BLOCK_SIZE*cps)+" bytes/s");
   }
   
   static double run(Runnable r){
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