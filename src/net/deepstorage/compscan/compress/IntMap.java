package net.deepstorage.compscan.compress;

import java.util.*;

class IntMap{
   private final int hashMask;
   private final int indexMask;
   private final int[] data;
   
   private boolean hasZeroKey;
   private int zeroValue;
   
   //size MUST be 2^n; capacity is size-1: at least one element MUST be empty
   public IntMap(int size){
      hashMask=size-1;
      indexMask=(size<<1)-1;
      data=new int[size<<1];
   }


   //same key must be used with the same hash
   void put(final int hash, final int key, final int value){
//System.out.println("put("+hash+","+key+","+value+")");
      if(key==0){
         hasZeroKey=true;
         zeroValue=value;
         return;
      }
      int i=(hash&hashMask)<<1;
      int k;
      while((k=data[i])!=0 && k!=key) i=(i+2)&indexMask;
      data[i]=key;
      data[i+1]=value;
   }
   
   public int get(int hash, int key, int defaultValue){
      if(key==0){
         if(hasZeroKey) return zeroValue;
         return defaultValue;
      }
      int i=(hash&hashMask)<<1;
      int k;
      for(; (k=data[i])!=0; i=(i+2)&indexMask){
         if(k==key) return data[i+1];
      }
      return defaultValue;
   }
   
   public void clear(){
      hasZeroKey=false;
      Arrays.fill(data,0);
   }
   
   public static void main(String[] args){
      IntMap map=new IntMap(16);
      for(int i=0;i<16;i++){
         map.put(2*i,i,i+1);
      }
      for(int i=0;i<16;i++){
         System.out.println(i+": "+map.get(2*i,i,-1));
      }
   }
   
}