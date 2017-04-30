package net.deepstorage.compscan.util.map;

import java.util.*;
import java.lang.reflect.*;
import java.io.*;
import java.nio.*;
import net.deepstorage.compscan.util.Util;

public class DirectMap extends ByteBufferMap{
   private final static Method getCleaner;
   private final static Method clean;
   static{
      try{
         ByteBuffer bb=ByteBuffer.allocateDirect(0);
         getCleaner=bb.getClass().getMethod("cleaner");
         clean=getCleaner.getReturnType().getMethod("clean");
         getCleaner.setAccessible(true);
         clean.setAccessible(true);
      }
      catch(Exception e){
         throw new Error(e);
      }
   }
   
   public DirectMap(
      int keySize, int valueSize, int addrSize, int maxListSize, int logBufSize
   ){
      super(keySize, valueSize, addrSize, maxListSize, logBufSize);
   }
   
   protected ByteBuffer createBuffer(int size){
      return ByteBuffer.allocateDirect(size);
   }
   
   protected void dispose(ByteBuffer bb){
      try{
         clean.invoke(getCleaner.invoke(bb));
      }
      catch(Exception e){
         System.out.println("WARNING: DirectMap: buffer disposal failed: "+e);
      }
   }
   
   protected void clearNewBlock(ByteBuffer b, int off, int size){
      //do nothing, new buffers are clean
   }
   
   public static void main(String[] args){
      System.out.println(getCleaner);
      System.out.println(clean);
      
      DirectMap map=new DirectMap(4, 8, 5, 3, 16);
      map.open();
      long addr=map.newBlock(1000,true);
      int N=1000_000_000;
      for(int i=N/2;i-->0;){
         map.write(addr,0,map.read(addr,0,8)+1,8);
      }
      long t0=System.currentTimeMillis();
      for(int i=N;i-->0;){
         map.write(addr,i&0xff,map.read(addr,i&0xff,8)+1,8);
      }
      long dt=System.currentTimeMillis()-t0;
      System.out.println(N+" read+writes : "+dt+" mls");
      System.out.println((N*1000f/dt)+" read+writes/s");
      
      byte[] key={1,2,3,4,5};
      for(int i=N/2;i-->0;){
         map.matches(addr,0,key,0,key.length);
      }
      t0=System.currentTimeMillis();
      for(int i=N;i-->0;){
         map.matches(addr,i&0xff,key,0,key.length);
      }
      dt=System.currentTimeMillis()-t0;
      System.out.println(N+" matches : "+dt+" mls");
      System.out.println((N*1000f/dt)+" matches/s");
      System.exit(0);
      
      map.write(0,0,new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19},0,20);
      System.out.println(map);
      System.out.println(map.matches(0,0,new byte[]{0,1,2,3,4,5,6,7,8,9,10},0,11));//t
      System.out.println(map.matches(0,1,new byte[]{0,1,2,3,4,5,6,7,8,9,10},0,11));//f
      System.out.println(map.matches(0,1,new byte[]{1,2,3,4,5,6,7,8,9,10},0,10));//t
      System.out.println(map.matches(0,5,new byte[]{5,6,7,8,9,10},0,6));//t
      System.out.println(map.matches(0,6,new byte[]{5,6,7,8,9,10},0,6));//f
      System.out.println();
      System.out.println(Long.toString(map.read(1,0,4),16));//0x04030201
      System.out.println(Long.toString(map.read(1,0,6),16));//0x060504030201
      System.out.println(Long.toString(map.read(1,0,8),16));//0x0807060504030201
      System.out.println(Long.toString(map.read(3,0,4),16));//0x06050403
      System.out.println(Long.toString(map.read(3,0,6),16));//0x080706050403
      System.out.println(Long.toString(map.read(3,0,8),16));//0x0a09080706050403
      map.write(0,21,0x0807060504030201L,8);
      map.write(0,30,0x0807060504030201L,7);
      map.write(0,38,0x0807060504030201L,6);
      map.write(0,45,0x0807060504030201L,5);
      map.write(0,51,0x0807060504030201L,4);
      map.write(0,56,0x0807060504030201L,3);
      map.write(0,60,0x0807060504030201L,2);
      map.write(0,63,0x0807060504030201L,1);
      System.out.println(Util.toString(map.data.get(0),0,70));
      //System.exit(0);
      
      map.dispose();
      map.open();
      
      DataMap.Cursor c=map.new Cursor();
      System.out.println(c.put(new byte[]{1,2,1,4},0));
      System.out.println(c.put(new byte[]{1,2,1,4},0));
      System.out.println(c.put(new byte[]{1,3,1,4},0));
      System.out.println(c.put(new byte[]{1,3,1,5},0));
      System.out.println(c.put(new byte[]{1,3,1,6},0));
      System.out.println(c.put(new byte[]{1,3,1,7},0));
//      System.out.println("map="+map.toString(true,false));
      System.out.println("map="+map.toString(false,true));
      System.out.println(" ===  ");
      System.out.println(c.seek(new byte[]{1,2,1,4},0)); //true
      System.out.println(c.seek(new byte[]{1,2,1,5},0)); //false
      System.out.println(c.seek(new byte[]{1,2,3,4},0)); //false
      System.out.println(c.seek(new byte[]{1,3,1,3},0)); //false
      System.out.println(c.seek(new byte[]{1,3,1,4},0)); //true
      System.out.println(c.seek(new byte[]{1,3,1,5},0)); //true
      System.out.println(c.seek(new byte[]{1,3,1,6},0)); //true
      System.out.println(c.seek(new byte[]{1,3,1,7},0)); //true
      System.out.println(c.seek(new byte[]{1,3,1,8},0)); //false
      System.out.println(" ===  ");
      c.scan((k)->{
         System.out.println(Util.toString(k,0,k.length)+" -> "+c.readValue(0,8));
      });
   }
}
