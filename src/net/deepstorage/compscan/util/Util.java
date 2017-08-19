package net.deepstorage.compscan.util;

import java.util.*;
import java.nio.ByteBuffer;

public class Util{
   public static final Set<Runnable> cleanupTasks=Collections.synchronizedSet(new HashSet<>());
   static{
      Runtime.getRuntime().addShutdownHook(new Thread(){
         public void run(){
            Executor.shutdownNow();
            synchronized(cleanupTasks){
               for(Runnable r:cleanupTasks){
                  try{
                     r.run();
                  }
                  catch(Exception e){}
               }
            }
         }
      });
   }
   
   public static String toString(byte[] data){
      return toString(data,0,data.length);
   }
   public static String toString(byte[] data, int off, int len){
      if(data==null) return null;
      StringBuilder sb=new StringBuilder("[");
      for(int i=0;i<len;i++){
         if(sb.length()>1) sb.append(',');
         sb.append(data[off+i]&0xff);
      }
      sb.append("]");
      return sb.toString();
   }
   
   public static String toString(int[] data){
      return toString(data,0,data.length);
   }
   public static String toString(int[] data, int off, int len){
      if(data==null) return null;
      StringBuilder sb=new StringBuilder("[");
      for(int i=0;i<len;i++){
         if(sb.length()>1) sb.append(',');
         sb.append(data[off+i]);
      }
      sb.append("]");
      return sb.toString();
   }
   
   public static String toHexString(byte[] data){
      return toHexString(data,0,data.length);
   }
   public static String toHexString(byte[] data, int off, int len){
      if(data==null) return null;
      StringBuilder sb=new StringBuilder("");
      for(int i=0;i<len;i++){
         int v=data[off+i]&0xff;
         sb.append(toHexChar(v>>4)).append(toHexChar(v));
      }
      return sb.toString();
   }
   
   private static char toHexChar(int i){
      i=i&0xf;
      if(i<10) return (char)('0'+i);
      return (char)('a'+i-10);
   }
   
   public static String toString(ByteBuffer data, int off, int len){
      if(data==null) return null;
      StringBuilder sb=new StringBuilder("[");
      for(int i=0;i<len;i++){
         if(sb.length()>1) sb.append(',');
         sb.append((data.get(off+i)&0xff));
      }
      sb.append("]");
      return sb.toString();
   }
   
   //how much bits is required to count d elements
   public static int log(long d){
      int v=0;
      for(d-=1;d>0;d>>=1) v++;
      return v;
   }
   
   //integer with symbolic suffix: \d+[kmgt]?
   public static Long parseSize(String spec){
      if(spec==null || spec.length()==0) return null;
      char lastChar=spec.charAt(spec.length()-1);
      long scale=1;
      if(Character.isLetter(lastChar)){
         spec=spec.substring(0,spec.length()-1);
         switch(Character.toLowerCase(lastChar)){
            case 'k': scale=1<<10; break;
            case 'm': scale=1<<20; break;
            case 'g': scale=1<<30; break;
            case 't': scale=1<<40; break;
         }
      }
      try{
         return Long.parseLong(spec)*scale;
      }
      catch(NumberFormatException e){
         throw new IllegalArgumentException("Incorrect format for BIG size specifier: "+spec);
      }
   }
   
   public static String formatSize(long n){
      int scale=0;
      while((n&1023)==0 && (scale<4)){
         scale++;
         n>>>=10;
      }
      return Long.toString(n)+" KMGTP".charAt(scale);
   }
   
   public static int max(int[] v){
      int max=v[0];
      for(int i=1;i<v.length;i++) max=Math.max(max,v[i]);
      return max;
   }
   
   public static void main(String[] args){
//      for(int i=0;i<16;i++) System.out.println(toHexChar(i));
//      System.out.println(toHexString(new byte[]{1,2,3,4,5},0,5));
//      System.out.println(toHexString(new byte[]{61,62,63,64,65},0,5));
//      System.out.println(log(0));//0
//      System.out.println(log(1));//0
//      System.out.println(log(3));//2
//      System.out.println(log(1<<20));//20
//      System.out.println(log(1L<<40));//40
      System.out.println(parseSize("128M"));
      System.out.println(formatSize(1<<27));
   }
}