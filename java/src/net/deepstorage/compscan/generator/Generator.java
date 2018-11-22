package net.deepstorage.compscan.generator;

import java.util.*;
import java.io.*;
import java.util.function.*;

import net.deepstorage.compscan.util.Util;
import net.deepstorage.compscan.CompressionInterface;
import net.deepstorage.compscan.compress.*;

public class Generator{
   private final CompressionInterface cpr;
   private final float ratio;
   private final int blockSize;
   private final int superblockSize;
   private final int poolSize;
   private final BlockOrder blockOrder;
   
   public Generator(
      CompressionInterface cpr, float ratio, int blockSize, int superblockSize
   ){
      this(cpr, ratio, blockSize, superblockSize, superblockSize*2);
   }
   
   public Generator(
      CompressionInterface cpr, float ratio, int blockSize, 
      int superblockSize, int poolSize
   ){
//System.out.println("new Generator("+cpr.getClass().getSimpleName()+","+ratio+","+blockSize+","+superblockSize+","+poolSize+")");
      this.cpr=cpr;
      this.ratio=ratio;
      this.blockSize=blockSize;
      this.superblockSize=superblockSize;
      this.poolSize=poolSize;
      //period: min|period*blockSize >= superblockSize
      //the same block shouldn't repat inside a superblock
      blockOrder=new AdvancedBlockOrder((superblockSize+blockSize-1)/blockSize);
   }
   
   //may be slow
   public void addGroup(int blocks, int repeats){
      blockOrder.addGroup(blocks, repeats, newBlockSequence());
   }
   
   public Enumeration<byte[]> build(){
      return blockOrder.toBlockSequence();
   }
   
   public int totalBlockCount(){
      return blockOrder.getBlockCount();
   }
   
   private Supplier<byte[]> newBlockSequence(){
      CyclicDataPool pool=new CyclicDataPool();
      while(pool.size()<poolSize) pool.add(CompressionUtil.generate(cpr, ratio, superblockSize));
      pool.clip(poolSize);
      return ()->pool.read(blockSize);
   }
   
   public static void main(String[] args) throws Exception{
      int blockSize=1<<12; //4k
      int superblockSize=blockSize*3; //12k
      CompressionInterface cpr=new LZ4();//GZIP();
      Generator g=new Generator(cpr,0.66f,blockSize,superblockSize);
      g.addGroup(1,103);
      g.addGroup(2,30);
      g.addGroup(4,15);
      g.addGroup(6,5);
      g.addGroup(10,2);
      g.addGroup(30,1);
      Enumeration<byte[]> blocks=g.build();
      System.out.println(g.blockOrder.getBlockCount()+", "+g.blockOrder.getUniqueBlockCount());
      OutputStream out=new FileOutputStream("../testdata/data/generated/0.66.dat");
      CyclicDataPool pool=new CyclicDataPool();
      int c=0;
      while(blocks.hasMoreElements()){
         byte[] block=blocks.nextElement();
         pool.add(block);
         out.write(block);
         System.out.print(AbstractBlockOrder.getMark(block)+"\t");
         if(++c>=3){
            System.out.println();
            c=0;
         }
      }
      out.close();
      int totalSize=pool.size();
      System.out.println(totalSize);
      for(int i=(totalSize+superblockSize-1)/superblockSize; i-->0;){
         byte[] sb=pool.read(superblockSize);
         System.out.println(sb.length+" -> "+cpr.compress(sb,-1).length);
      }
   }
}