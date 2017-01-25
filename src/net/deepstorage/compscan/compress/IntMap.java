package net.deepstorage.compscan.compress;

import java.util.Arrays;

class IntMap{
   private final int mask;
   private final int[] data;
   
   private boolean hasZeroKey;
   private int zeroValue;
   
   //size MUST be 2^n; capacity is size-1: at least one element MUST be empty
   public IntMap(int size){
      mask=size-1;
      data=new int[size<<1];
   }
   
   //same key must be used with the same hash
   void put(final int hash, final int key, final int value){
      if(key==0){
         hasZeroKey=true;
         zeroValue=value;
         return;
      }
      int i=(hash&mask)<<1;
      int k;
      while((k=data[i])!=0 && k!=key) i=(i+2)&mask;
      data[i]=key;
      data[i+1]=value;
   }
   
   public int get(int hash, int key, int defaultValue){
      if(key==0){
         if(hasZeroKey) return zeroValue;
         return defaultValue;
      }
      int i=(hash&mask)<<1;
      int k;
      for(; (k=data[i])!=0; i=(i+2)&mask){
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
      for(int i=0;i<15;i++){
         map.put(i*31,i,i+1);
      }
      for(int i=0;i<16;i++){
         System.out.println(i+": "+map.get(i*31,i,-1));
      }
   }
}