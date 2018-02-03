package net.deepstorage.compscan.generator;

import net.deepstorage.compscan.util.Util;

public class CyclicDataPool{
   private byte[] data;
   private int size, ptr;
   
   public int size(){
      return size;
   }
   
   public void add(byte[] block){
      add(block,0,block.length);
   }
   
   public void add(byte[] block, int off, int len){
      ensureCapacity(size+len);
      System.arraycopy(block,off,data,size,len);
      size+=len;
   }
   
   public void clip(int len){
      if(len>=size) return;
      size=len;
      ptr=Math.min(ptr,len);
   }
   
   public void pos(int pos){
      ptr=pos%size;
   }
   
   public void skip(int len){
      int remaining=size-ptr;
      if(len<remaining){
         ptr+=len;
         return;
      }
      len-=remaining;
      ptr=0;
      while(len>=size) len-=size;
      ptr=len;
   }
   
   public byte[] read(int len){
      byte[] buf=new byte[len];
      read(buf);
      return buf;
   }
   public void read(byte[] buf){
      read(buf, 0, buf.length);
   }
   public void read(byte[] buf, int off, int len){
      int remaining=size-ptr;
      if(len<remaining){
         System.arraycopy(data,ptr,buf,off,len);
         ptr+=len;
         return;
      }
      System.arraycopy(data,ptr,buf,off,remaining);
      ptr=0;
      off+=remaining;
      len-=remaining;
      while(len>size){
         System.arraycopy(data,0,buf,off,size);
         off+=size;
         len-=size;
      }
      System.arraycopy(data,0,buf,off,len);
      ptr=len;
   }
   
   private void ensureCapacity(int s){
      if(data==null){
         data=new byte[s*3/2+30];
         return;
      }
      if(s<=data.length) return;
      byte[] newdata=new byte[s*3/2+30];
      System.arraycopy(data,0,newdata,0,size);
      data=newdata;
   }
   
   public static void main(String[] args){
      CyclicDataPool pool=new CyclicDataPool();
      pool.add(new byte[]{1,2,3});
      pool.add(new byte[]{4,5,6,7});
      pool.add(new byte[]{8,9,10,11,12});
      System.out.println(Util.toString(pool.data,0,pool.size));
      byte[] buf=new byte[16];
      for(int i=3;i-->0;){
         pool.read(buf);
         System.out.println("  "+Util.toString(buf));
      }
      System.out.println("ptr="+pool.ptr);//12
      pool.skip(10);
      System.out.println("ptr="+pool.ptr);//10
      pool.clip(8);
      System.out.println("ptr="+pool.ptr);//8
      System.out.println(Util.toString(pool.read(5)));//1..5
   }
}
