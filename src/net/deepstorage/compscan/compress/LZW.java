package net.deepstorage.compscan.compress;

import net.deepstorage.compscan.CompressionInterface;

public class LZW implements CompressionInterface{
   static final int TABLE_SIZE=1<<12;
   
   private class Node{
      byte value;
      int code, hash;
      
      Node next(byte b){
         int i=indexMap.get(hash(b),key(b),-1);
         if(i<0) return null;
         return table[i];
      }
      
      Node add(byte b){
         int h=hash(b);
         int k=key(b);
         int code=nextCode();
         Node n=table[code];
         if(n==null) table[code]=n=new Node();
         n.value=b;
         n.code=code;
         n.hash=h;
         indexMap.put(h,k,code);
         return n;
      }
      
      private final int key(byte b){
         return (code<<8)|(b&0xff);
      }
      private final int hash(byte b){
         return (hash*317+b)*317;
      }
      
      public String toString(){
         return "Node(v="+value+", c="+code+", h="+hash+")";
      }
   }
   
   private final Node[] table=new Node[TABLE_SIZE];
   private final IntMap indexMap=new IntMap(TABLE_SIZE*4);
   
   final Node rootNode=new Node();
   
   private int currentCode, threshold, width;
   
   private final BitWriter bitWriter=new BitWriter();
   
   public LZW(){
      resetTable();
   }
   
   public void resetTable(){
      currentCode=0;
      threshold=256;
      width=8;
      indexMap.clear();
      for(int i=0;i<256;i++) rootNode.add((byte)i);
   }
   
   public byte[] compress(byte[] data, int blockSize){
//      resetTable(); //???
      bitWriter.reset();
      Node current=rootNode;
      for(int i=0;i<data.length;i++){
         byte b=data[i];
         Node n=current.next(b);
         if(n!=null){
            current=n;
            continue;
         }
         writeCode(current.code);
         if(currentCode<TABLE_SIZE) current.add(b);
//System.out.println("curr="+current+", next("+b+")="+current.next(b));
         current=rootNode.next(b);
//System.out.println("  curr -> "+current);
      }
      writeCode(current.code);
      return bitWriter.toArray();
   }
   
   private void writeCode(int c){
      bitWriter.write(c,width);
   }
   
   private int nextCode(){
//      currentCode++;
      if(currentCode>=threshold){
         threshold<<=1;
         width++;
      }
//      return currentCode;
      return currentCode++;
   }
   
   static int runs;
   
   public static void main(String[] args) throws Exception{
      LZW lzw=new LZW();
      byte[] data=new byte[2000];
      for(int i=1;i<data.length;i++) data[i]=(byte)(i+data[i-1]*37);
      
      byte[] cdata=lzw.compress(data, data.length);
      System.out.println("compression: "+data.length+" -> "+cdata.length);
      
      //heat up
//      byte[] tmp=new byte[10];
//      for(int i=30000;i-->0;) lzw.compress(tmp,tmp.length);
      
      runs=0;
      Runnable r=new Runnable(){ public void run(){
         lzw.compress(data,data.length);
         runs++;
      }};
      System.out.println("LZW speed: "+(SpeedTest.run(r)*data.length)+" B/s");
      System.out.println("currentCode: "+lzw.currentCode);
      
//      ByteArrayOutputStream buf=new ByteArrayOutputStream();
//      java.util.zip.GZIPOutputStream gzip=new java.util.zip.GZIPOutputStream(buf);
//      gzip.write(data);
//      gzip.close();
//      System.out.println("zipped: "+buf.size());
//      
////      FileOutputStream file=new FileOutputStream("data.bin");
////      file.write(data);
////      file.close();
   }
}
