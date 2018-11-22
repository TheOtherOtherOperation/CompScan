package net.deepstorage.compscan.generator;

import java.util.*;
import java.util.function.Supplier;

public abstract class AbstractBlockOrder<T extends AbstractBlockOrder.Group> implements BlockOrder{
   protected final int period;
   protected final List<T> entries=new ArrayList<>();
   
   private int nBlocks, nUniqueBlocks;
   
   //implementation should do its best for the same block not to appear 
   //more than once withing a period
   protected AbstractBlockOrder(int period){
      this.period=period;
   }
   
   public int getBlockCount(){
      return nBlocks;
   }
   
   public int getUniqueBlockCount(){
      return nUniqueBlocks;
   }
   
   /*
    * @param blocks number of unique blocks in the batch
    * @param repeats number of repeats for each unique block
    * @param data - supplies properly compressible blocks; no restriction here.
    * The implementation must enforce block uniqueness by adding proper currentMark
    * to the end of block
    */
   public void addGroup(int blocks, int repeats, Supplier<byte[]> data){
      entries.add(newGroup(nUniqueBlocks, blocks, repeats, data));
      nUniqueBlocks+=blocks;
      nBlocks+=blocks*repeats;
   }
   
   public abstract Enumeration<byte[]> toBlockSequence();
   
   protected abstract T newGroup(int currentMark, int blocks, int repeats, Supplier<byte[]> data);
   
   protected static void setMark(byte[] block, long currentMark){
      for(int i=8;i-->0;) block[block.length-i-1]=(byte)(currentMark>>(i<<3));
   }
   
   protected static long getMark(byte[] block){
      long currentMark=0;
      for(int i=8;i-->0;) currentMark|=(block[block.length-i-1]&0xffL)<<(i<<3);
      return currentMark;
   }
   
   protected static class Group{
      protected final int nRepeats;
      protected final int nUniqueBlocks;
      protected int remainingRepeats;
      protected int remainingUniqueBlocks;
      protected long currentMark;
      
      private Supplier<byte[]> src;
      private byte[] data;
      
      Group(long firstMark, int nBlocks, int nRepeats, Supplier<byte[]> src){
         this.src=src;
         this.currentMark=firstMark;
         this.nUniqueBlocks=nBlocks;
         this.nRepeats=nRepeats;
         this.remainingUniqueBlocks=nBlocks;
         if(nBlocks>0){
            remainingUniqueBlocks-=1;
            remainingRepeats=nRepeats;
         }
      }
      
      boolean hasBlock(){
         if(remainingRepeats>0) return true;
         if(nRepeats<=0 || remainingUniqueBlocks<=0) return false;
         remainingRepeats=nRepeats;
         remainingUniqueBlocks--;
         data=null;
         return true;
      }
      
      byte[] nextBlock(){
         if(data==null){
            data=src.get();
            setMark(data, currentMark);
            currentMark++;
         }
         remainingRepeats--;
         return data;
      }
      
      int remainingBlocks(){
         return remainingRepeats+remainingUniqueBlocks*nRepeats;
      }
   }
   
   public static void main(String[] args) {
      // TODO: Add your code here
   }   
}
