package net.deepstorage.compscan.util.map;

import java.util.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import net.deepstorage.compscan.util.Util;

import static net.deepstorage.compscan.util.MemUtil.*;

public abstract class ByteBufferMap extends DataMap{
   List<ByteBuffer> data;
   private final int logBufSize;
   private final int bufSize;
   private final int bufSizeMask;
   private final int maxBufCount;
   
   private int currOff;
   
//public int reads;
//public int writes;
//public int matches;
   
   public ByteBufferMap(
      int keySize, int valueSize, int addrSize, int maxListSize, int bufSize
   ){
      super(keySize, valueSize, addrSize, maxListSize);
      if(bufSize<=0) throw new IllegalArgumentException("unsupported bufSize: "+bufSize);
      this.bufSize=bufSize;
      logBufSize=Util.log(bufSize);
      bufSizeMask=(1<<logBufSize)-1;
      maxBufCount=1<<(addrSize*8-logBufSize);
   }
   
   protected abstract ByteBuffer createBuffer(int size);
   protected abstract void dispose(ByteBuffer b);
   protected abstract void clearNewBlock(ByteBuffer b, int off, int size);
   
   public void dispose(){
      for(int i=data.size(); i-->0;) dispose(data.remove(i));
      currOff=0;
   }
   
   protected long newBlock(int size, boolean clear){
      if(data==null) data=new ArrayList<ByteBuffer>();
      if(data.size()==0 || (currOff+size+8)>bufSize){
         if(data.size()>=maxBufCount) throw new RuntimeException("map overflow in "+getClass());
         ByteBuffer bb=createBuffer(bufSize);
         bb.order(ByteOrder.LITTLE_ENDIAN);
         data.add(bb);
         currOff=0;
      }
      int bufInd=data.size()-1;
      if(clear) clearNewBlock(data.get(bufInd), currOff, size);
//      long v=(bufInd<<logBufSize)|currOff;
      long v=(((long)bufInd)<<logBufSize)|currOff;
      currOff+=size;
      return v;
   }
   
   protected long read(long blockAddr, int off, int bytes){
//reads++;
      ByteBuffer buf=data.get((int)(blockAddr>>>logBufSize));
      int p=(int)(blockAddr&bufSizeMask)+off;
      return buf.getLong(p)&(-1L>>>((8-bytes)<<3));
   }
   
   protected int read(long blockAddr, int off, byte[] out, int outOff, int bytes){
//reads++;
      ByteBuffer buf=data.get((int)(blockAddr>>>logBufSize));
      buf.position((int)(blockAddr&bufSizeMask)+off);
      buf.get(out,outOff,bytes);
      return bytes;
   }
   
   protected void write(long blockAddr, int off, long value, int bytes){
//writes++;
      ByteBuffer buf=data.get((int)(blockAddr>>>logBufSize));
      int p=(int)(blockAddr&bufSizeMask)+off;
      long v=buf.getLong(p);
      long loMask=-1L>>>((8-bytes)<<3);
      buf.putLong(p,(v&~loMask)^(value&loMask));
   }
   
   protected void write(long blockAddr, int off, byte[] in, int inOff, int bytes){
//writes++;
      ByteBuffer buf=data.get((int)(blockAddr>>>logBufSize));
      buf.position((int)(blockAddr&bufSizeMask)+off);
      buf.put(in,inOff,bytes);
   }
   
   protected boolean matches(long blockAddr, int off, byte[] key, int keyOff, int keySize){
//matches++;
      ByteBuffer buf=data.get((int)(blockAddr>>>logBufSize));
      int p=(int)(blockAddr&bufSizeMask)+off;
      int shift=0;
      while((keySize-shift)>=8){
         if(readLong(key,keyOff+shift)!=buf.getLong(p+shift)) return false;
         shift+=8;
      }
      if((keySize-shift)>=4){
         if(readInt(key,keyOff+shift)!=buf.getInt(p+shift)) return false;
         shift+=4;
      }
      if((keySize-shift)>=2){
         if(readShort(key,keyOff+shift)!=buf.getShort(p+shift)) return false;
         shift+=2;
      }
      if((keySize-shift)>=1){
         return key[keyOff+shift]==buf.get(p+shift);
      }
      return true;
   }
   
   public long dataSize(){
      if(data==null) return -1;
      return (data.size()-1L)*bufSize+currOff;
   }
   
   public String toString(boolean data, boolean structure){
      List<ByteBuffer> list=this.data;
      int size= list==null? -1: list.size();
      String s=
         "DirM(\n"+
         "  bufSize="+bufSize+"\n"+
         "  logBufSize="+logBufSize+"\n"+
         "  maxBufCount="+maxBufCount+"\n"+
         "  N buffers="+size+"\n"+
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
      return toString(false,false);
   }
   
   public static void main(String[] args){
      
   }
}
