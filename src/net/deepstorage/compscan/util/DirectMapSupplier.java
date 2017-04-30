package net.deepstorage.compscan.util;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import net.deepstorage.compscan.util.map.*;
import net.deepstorage.compscan.util.Util;

public class DirectMapSupplier implements Supplier<MdMap>{
   protected int mdSize, addrSize, listSize, logChunkSize;
   
   public DirectMapSupplier(int mdSize, int listSize, int logChunkSize, long maxDataSize){
      this.mdSize=mdSize;
      this.listSize=listSize;
      this.logChunkSize=logChunkSize;
      this.addrSize=addrSizeForData(maxDataSize,logChunkSize,listSize);
   }
   
   public MdMap get(){
      return get(new DirectMap(mdSize, 8, addrSize, listSize, logChunkSize));
   }
   
   protected MdMap get(DataMap map){
      Runnable clean=()->{
         map.dispose();
      };
      Util.cleanupTasks.add(clean);
      return new MdMap(){
         private DirectMap.Cursor cursor=map.open().new Cursor();
         
         public int keyLength(){
            return mdSize;
         }
         public int size(){
            return map.size();
         }
         public long add(byte[] key, int off, long count){
            long v= cursor.put(key, off)? count: cursor.readValue(0,8)+count;
            cursor.writeValue(0,v,8);
            return v;
         }
         public long get(byte[] key, int off){
            if(cursor.seek(key, off)) return cursor.readValue(0,8);
            return 0;
         }
         public void scan(MdMap.Consumer feed){
            cursor.scan(md->{
               feed.consume(md,0,cursor.readValue(0,8));
            });
         }
         public void dispose(){
            Util.cleanupTasks.remove(clean);
            map.dispose();
         }
      };
   }
   
   public String toString(){
      return "direct DataMap, addr="+addrSize+", list="+listSize+", chunk="+Util.formatSize(1<<logChunkSize);
   }
   
   static int addrSizeForEntries(final long maxSize, final int logChunkSize, final int listSize){
      int addrSize0=8, addrSize;
      for(;;){
         int logMaxDataSize=Util.log((8+256*addrSize0)*maxSize/listSize);
         addrSize=((logChunkSize+7)>>3) + Math.max(1,(logMaxDataSize-logChunkSize+7)>>3);
         if(addrSize==addrSize0) break;
         addrSize0=addrSize;
      }
      return addrSize;
   }
   static int addrSizeForData(final long maxDataSize, final int logChunkSize, final int listSize){
      int logMaxDataSize=Util.log(maxDataSize);
      return ((logChunkSize+7)>>3) + Math.max(1,(logMaxDataSize-logChunkSize+7)>>3);
   }
   
   public static void main(String[] args){
      System.out.println(addrSizeForEntries(1L<<30,26,8));
      System.out.println(addrSizeForData(1L<<40,26,8));
   }
}