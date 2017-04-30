package net.deepstorage.compscan.util.map;

import java.util.*;
import java.io.*;
import net.deepstorage.compscan.util.Util;

import static net.deepstorage.compscan.util.MemUtil.*;

public class JavaHeapMap extends DataMap{
   private List<byte[]> data;
   private final int logBufSize;
   private final int bufSize;
   private final int bufSizeMask;
   
   private int currOff;
   
   public JavaHeapMap(
      int keySize, int valueSize, int addrSize, int maxListSize, int logBufSize
   ){
      super(keySize, valueSize, addrSize, maxListSize);
      this.logBufSize=logBufSize;
      bufSize=1<<logBufSize;
      bufSizeMask=bufSize-1;
   }
   
   public void dispose(){
      data.clear();
      currOff=0;
   }
   
   protected long newBlock(int size, boolean clear){
      if(data==null) data=new ArrayList<byte[]>();
      if(data.size()==0 || (currOff+size+8)>bufSize){ //8 is extra space for safely reading data in longs
         data.add(new byte[bufSize]);
         currOff=0;
      }
      int bufInd=data.size()-1;
      long v=(bufInd<<logBufSize)|currOff;
      currOff+=size;
      return v;
   }
   
   protected long read(long blockAddr, int off, int bytes){
      byte[] buf=data.get((int)(blockAddr>>logBufSize));
      int p=(int)(blockAddr&bufSizeMask)+off;
      return readLong(buf,p)&(-1L>>>((8-bytes)<<3));
   }
   
   protected int read(long blockAddr, int off, byte[] out, int outOff, int bytes){
      byte[] buf=data.get((int)(blockAddr>>logBufSize));
      int p=(int)(blockAddr&bufSizeMask)+off;
      System.arraycopy(buf,p,out,outOff,bytes);
      return bytes;
   }
   
   protected void write(long blockAddr, int off, long value, int bytes){
      byte[] buf=data.get((int)(blockAddr>>logBufSize));
      int p=(int)(blockAddr&bufSizeMask)+off;
      long v=readLong(buf,p);
      long loMask=-1L>>>((8-bytes)<<3);
      writeLong(buf,p,(v&~loMask)^(value&loMask));
   }
   
   protected void write(long blockAddr, int off, byte[] in, int inOff, int bytes){
      byte[] buf=data.get((int)(blockAddr>>logBufSize));
      int p=(int)(blockAddr&bufSizeMask)+off;
      System.arraycopy(in,inOff,buf,p,bytes);
   }
   
   protected boolean matches(long blockAddr, final int off, byte[] key, int keyOff, int keySize){
      byte[] buf=data.get((int)(blockAddr>>logBufSize));
      int p=(int)(blockAddr&bufSizeMask)+off;
      int shift=0;
      while((keySize-shift)>=8){
         if(readLong(key,keyOff+shift)!=readLong(buf,p+shift)) return false;
         shift+=8;
      }
      if((keySize-shift)>=4){
         if(readInt(key,keyOff+shift)!=readInt(buf,p+shift)) return false;
         shift+=4;
      }
      if((keySize-shift)>=2){
         if(readShort(key,keyOff+shift)!=readShort(buf,p+shift)) return false;
         shift+=2;
      }
      if((keySize-shift)>=1){
         return key[keyOff+shift]==buf[p+shift];
      }
      return true;
   }
   
   public long dataSize(){
      return (data.size()-1)*bufSize+currOff;
   }
   
   public String toString(boolean data, boolean structure){
      List<byte[]> list=this.data;
      String s=
         "JHM(\n"+
         "  bufSize="+bufSize+"\n"+
         "  N buffers="+this.data.size()+"\n"+
         "  currOff="+currOff+"\n"+
         "  data size="+dataSize()+"\n"+
         ")"
      ;
      if(data){
         s+="{\n";
         for(int i=0;i<list.size()-1;i++){
            s+=" "+i+": "+Util.toString(list.get(i),0,bufSize)+"\n";
         }
         s+=" "+(list.size()-1)+": "+Util.toString(list.get(list.size()-1),0,currOff)+"\n";
         s+="}\n";
      }
      else s+="\n";
      s+=super.toString(structure);
      return s;
   }
   
   public String toString(){
      return toString(true,true);
   }
   
   public static void main(String[] args){
      //DEBUG=true;
      //DataMap.DEBUG=true;
      
      JavaHeapMap map=new JavaHeapMap(4, 8, 5, 3, 16);
      map.open();
      
      map.write(0,0,new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19},0,20);
      System.out.println(map);
      System.out.println(map.matches(0,0,new byte[]{0,1,2,3,4,5,6,7,8,9,10},0,11));//t
      System.out.println(map.matches(0,1,new byte[]{0,1,2,3,4,5,6,7,8,9,10},0,11));//f
      System.out.println(map.matches(0,1,new byte[]{1,2,3,4,5,6,7,8,9,10},0,10));//t
      System.out.println(map.matches(0,5,new byte[]{5,6,7,8,9,10},0,6));//t
      System.out.println(map.matches(0,6,new byte[]{5,6,7,8,9,10},0,6));//f
      System.out.println();
      byte[] data=map.data.get(0);
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
      System.exit(0);
      
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
      c.scan((key)->{
         System.out.println(Util.toString(key,0,key.length)+" -> "+c.readValue(0,8));
      });
//      System.out.println("  ===  ");
   }
}