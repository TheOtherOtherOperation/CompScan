package net.deepstorage.compscan.generator;

import java.io.*;
import java.util.*;
import java.util.function.*;

import net.deepstorage.compscan.util.Util;

public class AdvancedBlockOrder 
   extends AbstractBlockOrder<AdvancedBlockOrder.Group>
   implements Enumeration<byte[]>
{
   protected final Comparator<Group> byRemainingBlocks=(e1,e2)->{
      //longer series come first
      return -Integer.compare(e1.remainingBlocks(), e2.remainingBlocks());
   };
   
   private int position; //number of supplied blocks
   
   private int index; //points to an entry wich provides next block
   
   public AdvancedBlockOrder(int period){
      super(period);
   }
   
   public boolean hasMoreElements(){
      for(;;){
         if(entries.size()==0){
            index=-1;
            return false;
         }
         index=0;
         Group oldestGroup=entries.get(0);
         for(int i=Math.min(entries.size(), period); i-->1;){
            if(entries.get(i).position < oldestGroup.position){
               index=i;
               oldestGroup=entries.get(i);
            }
         }
         if(!oldestGroup.hasBlock()){
            entries.remove(index);
            Collections.sort(entries, byRemainingBlocks);
            continue;
         }
         //avoid uneven depletion
         if(
            entries.size()>period &&
            oldestGroup.remainingBlocks() < entries.get(period).remainingBlocks()/2
         ){
            Collections.sort(entries, byRemainingBlocks);
            continue;
         }
         return true;
      }
   }
   
   public byte[] nextElement(){
      Group batch=entries.get(index);
      byte[] block=batch.nextBlock();
      batch.position=position++;
      return block;
   }
   
   public Enumeration<byte[]> toBlockSequence(){
      Collections.sort(entries, byRemainingBlocks);
      index=-1;
      return this;
   }
   
   protected Group newGroup(int idGen, int blocks, int repeats, Supplier<byte[]> data){
      return new Group(idGen, blocks, repeats, data);
   }
   
   static class Group extends AbstractBlockOrder.Group{
      int position=-1;
      
      Group(int idGen, int blocks, int repeats, Supplier<byte[]> data){
         super(idGen, blocks, repeats, data);
      }
      
      public String toString(){
         return "Group{"+currentMark+","+position+","+remainingBlocks()+"}";
      }
   }
   
   public static void main(String[] args){
      AdvancedBlockOrder g=new AdvancedBlockOrder(3);
      g.addGroup(1,10,()->new byte[12]);
      g.addGroup(2,5,()->new byte[12]);
      g.addGroup(3,3,()->new byte[12]);
      g.addGroup(4,2,()->new byte[12]);
      g.addGroup(1,5,()->new byte[12]);
      
      Enumeration<byte[]> seq=g.toBlockSequence();
      
      final int per=3;
      int c=0;
     
      while(g.hasMoreElements()){
         byte[] block=seq.nextElement();
         System.out.print(getMark(block)+"\t");
         if(++c>=per){
            System.out.println();
            c=0;
         }
      }
   }
}