package net.deepstorage.compscan.util.map;

import net.deepstorage.compscan.util.Util;
import java.util.function.*;

public abstract class DataMap{
static boolean DEBUG;
   private static final int LIST=1;
   private static final int TABLE=2;
   
   protected final int keySize;
   protected final int valueSize;
   protected final int blockAddrSize;
   
   protected final int maxListSize;
   protected final int listBlockSize;
   protected final int tableBlockSize;
   protected long dataEntryAddr;
   
   private final long[] addrTableBuffer=new long[256];
   
   private int size=0;
   
   protected DataMap(
      int keySize, int valueSize, int blockAddrSize, int maxListSize
   ){
      this.keySize=keySize;
      this.valueSize=valueSize;
      this.blockAddrSize=blockAddrSize;
      this.maxListSize=maxListSize;
      this.tableBlockSize=1+256*blockAddrSize; //type+map
      this.listBlockSize=1+blockAddrSize+keySize+valueSize; //type+nextaddr+key+value
   }
   
   //must be called before usage
   public DataMap open(){
      dataEntryAddr=newTableBlock();
      return this;
   }
   
   public class Cursor{
      private long currentBlock;
      private int currentOff;
      private final byte[] keyBuffer=new byte[keySize];
      
      public boolean seek(byte[] keyData, int keyOff){
         return seek(dataEntryAddr, 0, keyData, keyOff);
      }
      
      //false: entry exists
      public boolean put(byte[] keyData, int keyOff){
         return put2table(dataEntryAddr, 0, keyData, keyOff);
      }
      
      public long readValue(int off, int bytes){
         return read(currentBlock, currentOff+off, bytes);
      }
      
      public int readValue(int off, byte[] buf, int bufOff, int bytes){
         return read(currentBlock, currentOff+off, buf, bufOff, bytes);
      }
      
      public void writeValue(int off, long value, int bytes){
         write(currentBlock, currentOff+off, value, bytes);
      }
      
      public void writeValue(int off, byte[] buf, int bufOff, int bytes){
         write(currentBlock, currentOff+off, buf, bufOff, bytes);
      }
      
      private boolean seek(long blockAddr, int level, byte[] keyData, int keyOff){
         int type=(int)read(blockAddr, 0, 1);
         switch(type){
            case LIST:
               while(blockAddr!=0){
                  if(matches(blockAddr, 1+blockAddrSize+level, keyData, keyOff+level, keySize-level)){
                     currentBlock=blockAddr;
                     currentOff=1+blockAddrSize+keySize;
                     return true;
                  }
                  blockAddr=read(blockAddr,1,blockAddrSize);
               }
               return false;
            case 2: //table
               int b=keyData[keyOff+level]&0xff;
               long nextAddr=read(blockAddr, 1+b*blockAddrSize, blockAddrSize);
               if(nextAddr==0) return false;
               return seek(nextAddr, level+1, keyData, keyOff);
         }
         throw new Error("bad type: "+type+" at block #"+blockAddr);
      }
      
      private boolean put(
         long blockAddr, final int level,
         final long parentBlock, final int parentOff,
         final byte[] keyData, final int keyOff
      ){
         if(blockAddr==0){
            blockAddr=newListBlock();
            write(parentBlock,parentOff,blockAddr,blockAddrSize);
            write(blockAddr,1+blockAddrSize,keyData,keyOff,keySize);
            currentBlock=blockAddr;
            currentOff=1+blockAddrSize+keySize;
            size++;
            return true;
         }
         int type=(int)read(blockAddr, 0, 1);
         switch(type){
            case LIST:
               int n=0;
               long addr=blockAddr;
               long last=blockAddr;
               while(addr!=0){
                  if(matches(addr,1+blockAddrSize+level,keyData,keyOff+level,keySize-level)){
                     currentBlock=addr;
                     currentOff=1+blockAddrSize+keySize;
                     return false;
                  }
                  last=addr;
                  addr=read(addr,1,blockAddrSize);
                  n++;
               }
               
               //list is short, add new list item
               if(n<maxListSize) return put(0, level, last, 1, keyData, keyOff);
               
               //otherwise allocate new table, map and relink old items
               blockAddr=list2table(blockAddr, level);
               write(parentBlock,parentOff,blockAddr,blockAddrSize);
               //go on, put to table
               
            case TABLE:
               return put2table(blockAddr, level, keyData, keyOff);
         }
         throw new IllegalStateException("Bad type: "+type+" at "+blockAddr);
      }
      
      private boolean put2table(long table, int level, byte[] keyData, int keyOff){
         int b=keyData[keyOff+level]&0xff;
         int off=1+b*blockAddrSize;
         long nextAddr=read(table,off,blockAddrSize);
         return put(nextAddr, level+1, table, off, keyData, keyOff);
      }
      
      public void scan(Consumer<byte[]> feed){
         scan(dataEntryAddr, keyBuffer, feed);
      }
      
      private void scan(long blockAddr, byte[] keyBuf, Consumer<byte[]> feed){
         final int type=(int)read(blockAddr, 0, 1);
         switch(type){
            case LIST:{
               while(blockAddr!=0){
                  read(blockAddr, 1+blockAddrSize, keyBuf, 0, keySize);
                  currentBlock=blockAddr;
                  currentOff=1+blockAddrSize+keySize;
                  feed.accept(keyBuf);
                  blockAddr=read(blockAddr,1,blockAddrSize);
               }
               return;
            }
            case TABLE:{
               int p=1;
               for(int i=0;i<256;p+=blockAddrSize,i++){
                  long nextAddr=read(blockAddr, p, blockAddrSize);
                  if(nextAddr==0) continue;
                  scan(nextAddr, keyBuf, feed);
               }
               return;
            }
         }
      }
      
      void print(){
         System.out.println("currentBlock="+currentBlock+", currentOff="+currentOff);
         System.out.println(blockToString(currentBlock, false));
      }
   }
   //clean
   long newTableBlock(){
      long addr=newBlock(tableBlockSize, true);
      write(addr, 0, TABLE, 1);
      return addr;
   }
   
   //must fill next addr
   long newListBlock(){
      long addr=newBlock(listBlockSize, false);
      write(addr, 0, LIST, 1); //type
      //write(addr, 1, 0, blockAddrSize); //next addr
      return addr;
   }
   
   private long list2table(long addr, int level){
      long[] tails=addrTableBuffer;
      long table=newTableBlock();
      while(addr!=0){
         long next=read(addr,1,blockAddrSize);
         int b=(int)read(addr,1+blockAddrSize+level,1);
         int off=1+b*blockAddrSize;
         long list=read(table,off,blockAddrSize);
         if(list==0){
            tails[b]=addr;
            write(table,off,addr,blockAddrSize);//insert to table
            write(addr,1,0,blockAddrSize);//last item
         }
         else{
            long tail=tails[b];
            write(tail,1,addr,blockAddrSize);//append
            write(addr,1,0,blockAddrSize);//last item
            tails[b]=addr;
         }
         addr=next;
      }
      return table;
   }
   
   public int size(){
      return size;
   }
   
   public abstract void dispose();
   
   protected abstract long newBlock(int size, boolean clean);
   protected abstract long read(long blockAddr, int off, int bytes);
   protected abstract int read(long blockAddr, int off, byte[] data, int dataOff, int bytes);
   protected abstract void write(long blockAddr, int off, long value, int bytes);
   protected abstract void write(long blockAddr, int off, byte[] data, int dataOff, int bytes);
   protected abstract boolean matches(long blockAddr, int off, byte[] key, int keyOff, int keySize);
   protected abstract long dataSize();
   
   public String toString(){
      return toString(false);
   }
   
   public String toString(boolean blocks){
      String s="DataMap(\n"+
         "keySize="+keySize+"\n"+
         "valueSize="+valueSize+"\n"+
         "blockAddrSize="+blockAddrSize+"\n"+
         "dataEntryAddr="+dataEntryAddr+"\n"+
         "maxListSize="+maxListSize+"\n"+
         "listBlockSize="+listBlockSize+"\n"+
         "tableBlockSize="+tableBlockSize+"\n"+
         "size="+size+"\n"+
         ")\n"
      ;
      if(blocks){
         s+="blocks:\n";
         s+=blockToString(dataEntryAddr,true);
      }
      return s;
   }
   private String blockToString(long addr, boolean recurse){
      int type=(int)read(addr,0,1);
      byte[] buf=new byte[Math.max(keySize,valueSize)];
      switch(type){
         case LIST:{
            String s=addr+":{list:\n";
            while(addr!=0){
               read(addr,1+blockAddrSize,buf,0,keySize);
               s+="    "+Util.toString(buf,0,keySize)+":";
               read(addr,1+blockAddrSize+keySize,buf,0,valueSize);
               s+=Util.toString(buf,0,valueSize)+"\n";
               addr=read(addr,1,blockAddrSize);
            }
            return s+"}\n";
         }
         case 2:
            String s=addr+":{table:\n";
            for(int i=0;i<256;i++){
               long ref=read(addr,1+i*blockAddrSize,blockAddrSize);
               if(ref!=0){
                  s+="    "+i+": "+ref+"\n";
               }
            }
            s+="}\n";
            if(recurse) for(int i=0;i<256;i++){
               long ref=read(addr,1+i*blockAddrSize,blockAddrSize);
               if(ref!=0){
                  s+=blockToString(ref, recurse)+"\n";
               }
            }
            return s;
      }
      return addr+":<bad type: "+type+">";
   }
   
   
   public static void main(String[] args){
      
   }
}