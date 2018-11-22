package net.deepstorage.compscan.util;

import java.util.*;
import java.util.function.*;

public class JavaMapSupplier implements Supplier<MdMap>{
   private int mdSize;
   
   public JavaMapSupplier(int mdSize){
      this.mdSize=mdSize;
   }
   
   public MdMap get(){
      return new MdMap(){
         private final HashMap<MD,Long> map=new HashMap<>();
         
         public int keyLength(){
            return mdSize;
         }
         public int size(){
            return map.size();
         }
         public long add(byte[] key, int off, long count){
            MD md=new MD(key, off, mdSize);
            Long v=map.get(md);
            v= v==null? count: v+count;
            map.put(md, v);
            return v;
         }
         public long get(byte[] key, int off){
            Long v=map.get(new MD(key, off, mdSize));
            if(v==null) return 0;
            return v;
         }
         public void scan(Consumer feed){
            for(Map.Entry<MD,Long> e: map.entrySet()){
               feed.consume(e.getKey().data, e.getKey().off, e.getValue());
            }
         }
         public void dispose(){
            map.clear();
         }
      };
   }
   
   public String toString(){
      return HashMap.class.getName();
   }
}
