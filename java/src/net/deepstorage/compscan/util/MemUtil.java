package net.deepstorage.compscan.util;

import java.util.*;

import java.lang.reflect.*;
import sun.misc.Unsafe;

public class MemUtil {
   private static final Unsafe unsafe;
   private static final int arrayOffset;
   static{
      try{
         Field f=Unsafe.class.getDeclaredField("theUnsafe");
         f.setAccessible(true);
         unsafe=(Unsafe)f.get(null);
         arrayOffset=unsafe.arrayBaseOffset(byte[].class);
      }
      catch(Exception e){
         throw new Error(e);
      }
   }
   
   public static short readShort(byte[] buf, int off){
      return unsafe.getShort(buf,arrayOffset+off);
   }
   
   public static int readInt(byte[] buf, int off){
      return unsafe.getInt(buf,arrayOffset+off);
   }
   
   public static long readLong(byte[] buf, int off){
      return unsafe.getLong(buf,arrayOffset+off);
   }
   
   public static void writeLong(byte[] buf, int off, long value){
      unsafe.putLong(buf,arrayOffset+off,value);
   }
}