package net.deepstorage.compscan.generator;

import java.io.*;
import java.util.*;
import java.util.function.*;

import net.deepstorage.compscan.util.Util;

public class SimpleBlockOrder extends AbstractBlockOrder<AbstractBlockOrder.Group> implements Enumeration<byte[]>{
   protected final Comparator<Group> byTotalBlocks=(e1,e2)->{
      //longer series come first
      return -Integer.compare(e1.nRepeats*e1.nUniqueBlocks, e2.nRepeats*e2.nUniqueBlocks);
   };
   
   protected int index;
   
   public SimpleBlockOrder(int period){
      super(period);
   }
   
   protected Group newGroup(int idGen, int blocks, int repeats, Supplier<byte[]> data){
      return new Group(idGen, blocks, repeats, data);
   }
   
   public Enumeration<byte[]> toBlockSequence(){
      Collections.sort(entries, byTotalBlocks);
      index=0;
      return this;
   }
   
   public boolean hasMoreElements(){
      while(index<entries.size()){
         if(entries.get(index).hasBlock()) return true;
         entries.remove(index);
      }
      while(entries.size()>0){
         if(entries.get(0).hasBlock()){
            index=0;
            return true;
         }
         entries.remove(0);
      }
      return false;
   }
   
   public byte[] nextElement(){
      return entries.get(index++).nextBlock();
   }
   
   public static void main(String[] args){
//      Group e=new Group(7,3,3,()->new byte[]{1,0,0,0,0,0,0,0,0,0});
//      int c=1;
//      while(e.hasBlock()){
//         System.out.println(c+": "+Util.toString(e.nextBlock()));
//         c++;
//      }
//      System.exit(0);
      
      SimpleBlockOrder g=new SimpleBlockOrder(3);
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
