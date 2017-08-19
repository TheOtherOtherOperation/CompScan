package net.deepstorage.compscan.util.map;

import java.util.*;
import java.io.*;
import java.util.function.Function;
import net.deepstorage.compscan.util.Util;

public class MapTests {
   static interface IntStream{
      int next();
      void close();
   }
   
   static IntStream randomStream(boolean save){
      try{
         PrintWriter out= save? new PrintWriter(new FileWriter("random")): null;
         return new IntStream(){
            Random r=new Random();
            
            public int next(){
               int i=r.nextInt();
               if(save) out.println(i);
               return i;
            }
            
            public void close(){
               try{
                  if(save) out.close();
               }
               catch(Exception e){}
            }
         };
      }
      catch(Exception e){
         throw new Error(e);
      }
   }
   
   static IntStream savedStream(){
      try{
         BufferedReader in=new BufferedReader(new FileReader("random"));
         return new IntStream(){
            String line;
            
            public int next(){
               try{
                  return Integer.parseInt(in.readLine());
               }
               catch(Exception e){
                  throw new Error(e);
               }
            }
            
            public void close(){
               try{
                  in.close();
               }
               catch(Exception e){}
            }
         };
      }
      catch(Exception e){
         throw new Error(e);
      }
   }
   
   static IntStream uniqueStream(){
      return new IntStream(){
         int seed=123;
         public int next(){
            seed^=seed<<13;
            seed^=seed>>17;
            seed^=seed<<5;
            return seed;
         }
         public void close(){}
      };
   }
 
   public static void main(String[] args) throws Exception{
//System.in.read();
      File tmpDir=new File("mem");
      if(!tmpDir.exists()) tmpDir.mkdir();
      Function<Integer,Function<Integer,Function<Integer,DataMap>>> sup=
         //kSize->vSize->addSize->new DirectMap(kSize,vSize,addSize,10,1<<24).open()
         kSize->vSize->addSize->new FsMap(kSize,vSize,addSize,10,1<<24,tmpDir).open()
      ;
//      simpleTest(sup);
//      testRand();
//      compare();
//      speed(sup);
      
      speedSha1(3_000_000, 3);
      //size();
   }
   
   static void simpleTest(){
      simpleTest(kSize->vSize->addSize->new DirectMap(kSize,vSize,addSize,10,1<<17).open());
   }
   static void simpleTest(Function<Integer,Function<Integer,Function<Integer,DataMap>>> sup){
      DataMap map=sup.apply(4).apply(8).apply(5);
      
      byte[] key1=new byte[]{123,0,0,0};
      byte[] key2=new byte[]{-4,111,-29,1};
      byte[] key3=new byte[]{-78,-25,-121,-17};
      DataMap.Cursor c=map.new Cursor();
      System.out.println(c.put(key1,0));
      System.out.println(c.put(key2,0));
      System.out.println(c.put(key3,0));
      System.out.println(" ===  ");
      System.out.println(c.seek(key1,0));
      System.out.println("map="+map.toString(true));
      System.out.println("  ===  ");
      
      System.out.println(c.seek(key2,0));
      System.out.println(c.seek(key3,0));
      
      System.out.println("  ===  ");
      c.scan((key)->{
         System.out.println(Util.toString(key,0,key.length)+" -> "+c.readValue(0,8));
      });
   }
   
   static void testRand(){
      testRand(kSize->vSize->addSize->new DirectMap(kSize,vSize,addSize,10,1<<17).open());
   }
   static void testRand(Function<Integer,Function<Integer,Function<Integer,DataMap>>> sup){
      byte[] data=new byte[1<<20];
      int seed=123;
      for(int i=0;i<data.length;i+=4){
         data[i]=(byte)seed;
         data[i+1]=(byte)(seed>>8);
         data[i+2]=(byte)(seed>>16);
         data[i+3]=(byte)(seed>>24);
         seed^=seed<<13;
         seed^=seed>>17;
         seed^=seed<<5;
      }
      
      DataMap map=sup.apply(4).apply(8).apply(5);
      DataMap.Cursor c=map.new Cursor();
      int nput=0;
      try{
         for(int i=0;i<=(data.length/2-4);i+=4){
            //System.out.println("put "+Util.toString(data,i,4));
            if(!c.put(data,i)){
               System.out.println("key exists at "+i);
               break;
            }
            nput++;
         }
      }
      catch(OutOfMemoryError e){
         System.out.println("OOME, map="+map.toString(false));
         return;
      }
      
      int nfound=0, nmissing=0;
      int firstFound=Integer.MAX_VALUE;
      int lastFound=-1;
      int firstMissing=Integer.MAX_VALUE;
      int lastMissing=-1;
      for(int i=0;i<=(data.length-4);i+=4){
//         System.out.println("get "+Util.toString(data,i,4));
         if(c.seek(data,i)){
            nfound++;
            firstFound=Math.min(firstFound,i);
            lastFound=Math.max(lastFound,i);
         }
         else{
            nmissing++;
            firstMissing=Math.min(firstMissing,i);
            lastMissing=Math.max(lastMissing,i);
         }
      }
      
      int nadded=0;
      int firstAdded=Integer.MAX_VALUE;
      int lastAdded=-1;
      try{
         for(int i=0;i<=(data.length-4);i+=4){
            if(c.put(data,i)){
               nadded++;
               firstAdded=Math.min(firstAdded,i);
               lastAdded=Math.max(lastAdded,i);
            }
         }
      }
      catch(Error e){
         System.out.println("OOME, map="+map.toString(false));
         return;
      }
      System.out.println("  ===  ");
      System.out.println("map="+map.toString(false));
      System.out.println("  ===  ");
      
      System.out.println("all: "+data.length/4);
      System.out.println("nput: "+nput);
      System.out.println("nfound: "+nfound);
      System.out.println("nmissing: "+nmissing);
      System.out.println("firstFound: "+firstFound);
      System.out.println("lastFound: "+lastFound);
      System.out.println("firstMissing: "+firstMissing);
      System.out.println("lastMissing: "+lastMissing);
      System.out.println();
      System.out.println("nadded: "+nadded);
      System.out.println("firstAdded: "+firstAdded);
      System.out.println("lastAdded: "+lastAdded);
      
      int[] count=new int[1];
      c.scan((key)->{
         count[0]++;
      });
      System.out.println("count: "+count[0]);
   }
   
   static void compare(){
      compare(kSize->vSize->addSize->new DirectMap(kSize,vSize,addSize,10,1<<17).open());
   }
   static void compare(Function<Integer,Function<Integer,Function<Integer,DataMap>>> sup){
      HashMap<Integer,Integer> map1=new HashMap<>();
      DataMap map2=sup.apply(4).apply(8).apply(5);
      DataMap.Cursor c=map2.new Cursor();
//int N=1000;
//int N=100_000;
      int N=1000_000;
      {
         byte[] key=new byte[4];
//         IntStream stream=randomStream(true);
         IntStream stream=randomStream(false);
//         IntStream stream=savedStream();
         for(int i=N;i-->0;){
            int k=stream.next();
            key[0]=(byte)k;
            key[1]=(byte)(k>>8);
            key[2]=(byte)(k>>16);
            key[3]=(byte)(k>>24);
            Integer v1=map1.get(k);
            if(v1==null) v1=0;
            map1.put(k,v1+1);
            int v2;
            if(c.put(key,0)) v2=0;
            else v2=(int)c.readValue(0,4);
            c.writeValue(0,v2+1,4);
            if(v1!=v2) throw new Error("mismatch at key "+k+" map1.size="+map1.size()+" v1="+v1+", v2="+v2);
//if(k==1850755611){
//   System.out.println("key "+k+"("+Util.toString(key,0,key.length)+")");
//   System.out.println("  v1="+v1+", v2="+v2);
//   System.out.println("  m2 curr value="+c.readValue(0,4));
//   c.print();
//}
         }
         stream.close();
      }
//System.out.println(c.seek(new byte[]{27,74,80,110},0)); //1850755611
//System.out.println(c.readValue(0,4));
//c.print();
//System.exit(0);
      int[] count=new int[1];
      c.scan((key)->{
         count[0]++;
         int v2=(int)c.readValue(0,4);
         int k=(key[0]&0xff)|((key[1]&0xff)<<8)|((key[2]&0xff)<<16)|(key[3]<<24);
         Integer v1=map1.get(k);
         if(v1!=v2){
            System.out.println("mismatch at key "+k+" at count="+count[0]);
            System.out.println("v1="+v1+", v2="+v2);
            c.print();
         }
//         if(v1==null) throw new Error("null v1 at key "+k+", v2="+v2+" at count="+count[0]);
//         if(v1!=v2) throw new Error(v1+"!="+v2+" at key "+k+" at count="+count[0]);
      });
      System.out.println("map1: "+map1.size());
      System.out.println("map2: "+map2.size());
   }
   
   static void speed(){
      speed(kSize->vSize->addrSize->new DirectMap(kSize,vSize,addrSize,8,1<<24).open());
   }
   static void speed(Function<Integer,Function<Integer,Function<Integer,DataMap>>> sup){
      int N=4_000_000;
      DataMap map=sup.apply(4).apply(8).apply(5);
      byte[] data=new byte[N*4];
      int seed=123;
      for(int i=0;i<data.length;i+=4){
         data[i]=(byte)seed;
         data[i+1]=(byte)(seed>>8);
         data[i+2]=(byte)(seed>>16);
         data[i+3]=(byte)(seed>>24);
         seed^=seed<<13;
         seed^=seed>>17;
         seed^=seed<<5;
      }
      
      DataMap.Cursor c=map.new Cursor();
      for(int i=0;i<=(data.length/4-4);i+=4) c.put(data,i);
      
      int nput=0;
      long t0=System.currentTimeMillis();
      for(int i=0;i<=(data.length-4);i+=4){
         c.put(data,i);
         nput++;
      }
      long dt=System.currentTimeMillis()-t0;
      
      System.out.println("size: "+nput);
      System.out.println("time: "+dt);
      System.out.println("op/s: "+(nput*1000.0/dt));
   }
   
   static void speedSha1(int nKeys, int nThreads) throws Exception{
      DataMap[] maps=new DataMap[nThreads];
      byte[] data=new byte[20];
      for(int i=nThreads;i-->0;){
         DataMap map=maps[i]=new DirectMap(20,8,6,10,1<<25).open();
         DataMap.Cursor c=map.new Cursor();
         fillRandom(c,data,123,nKeys/4);
      }
      
      Thread[] threads=new Thread[nThreads];
      for(int i=nThreads;i-->0;){
         DataMap.Cursor c=maps[i].new Cursor();
         threads[i]=new Thread(){
            public void run(){
               fillRandom(c,new byte[20],123,nKeys);
            }
         };
      }
      
      long t0=System.currentTimeMillis();
      for(int i=nThreads;i-->0;) threads[i].start();
      for(int i=nThreads;i-->0;) threads[i].join();
      long dt=System.currentTimeMillis()-t0;
      
      System.out.println("nThreads: "+nThreads);
      System.out.println("size: "+maps[0].size());
      System.out.println("data size: "+maps[0].dataSize());
      System.out.println("time: "+dt);
      System.out.println("op/s: "+(nKeys*1000.0*nThreads/dt));
   }
      
      
   private static void fillRandom(DataMap.Cursor cursor, byte[] keybuf, int seed, int N){
      for(int i=N;i-->0;){
         keybuf[0]=(byte)seed;
         keybuf[1]=(byte)(seed>>8);
         keybuf[2]=(byte)(seed>>16);
         keybuf[3]=(byte)(seed>>24);
         boolean b=cursor.put(keybuf,0);
         if(b) cursor.writeValue(0,0,8);
         else cursor.writeValue(0,cursor.readValue(0,8)+1,8);
         seed^=seed<<13;
         seed^=seed>>17;
         seed^=seed<<5;
      }
   }
      
   
   
   static void size(){
      size(kSize->vSize->addSize->new DirectMap(kSize,vSize,addSize,8,1<<24).open());
   }
   static void size(Function<Integer,Function<Integer,Function<Integer,DataMap>>> sup){
      DataMap map=sup.apply(4).apply(8).apply(5);
      int N=20_000_000;
      int reportCycle=Math.max(N/1000,1);
      byte[] data=new byte[N*4];
      IntStream stream=randomStream(false);
      for(int i=0;i<data.length;i+=4){
         int v=stream.next();
         data[i]=(byte)v;
         data[i+1]=(byte)(v>>8);
         data[i+2]=(byte)(v>>16);
         data[i+3]=(byte)(v>>24);
      }
      
      DataMap.Cursor c=map.new Cursor();
      int size=0;
      try{
         for(int i=0;i<=(data.length-4);i+=4){
            if(c.put(data,i)) size++;
            if((size%reportCycle)==0) System.out.println("size="+size+", dataSize="+map.dataSize()+", bytes/key="+(map.dataSize()/size));
         }
      }
      catch(OutOfMemoryError e){
         System.out.println("OOME, map="+map.toString(false));
         return;
      }
      if(size!=map.size()) throw new Error(size+"!="+map.size());
   }
}