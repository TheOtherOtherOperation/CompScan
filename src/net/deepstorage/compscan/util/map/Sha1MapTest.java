package net.deepstorage.compscan.util.map;

import java.lang.reflect.Field;
import net.deepstorage.compscan.SHA1Encoder;
import net.deepstorage.compscan.util.Util;

public class Sha1MapTest{
   static long data=-1;
   static long keys=-1;
   static int list=10;
   static int buf=1<<27;
   static boolean fixed=false;
   
   public static void main(String[] args) throws Exception{
      Class vm=Class.forName("sun.misc.VM");
      Field directMemory=vm.getDeclaredField("directMemory");
      directMemory.setAccessible(true);
      directMemory.set(null,1L<<40);
//runFixed(10, 200_000_000);
//runFixed(20, 200_000_000);
//runFixed(30, 200_000_000);
//runFixed(40, 200_000_000);
//runFixed(60, 200_000_000);
//System.exit(0);
      
      for(int i=0;i<args.length;i++){
         switch(args[i]){
            case "-fixed": fixed=true; break;
            case "-data": data=Util.parseSize(args[++i]); break;
            case "-keys": keys=Util.parseSize(args[++i]); break;
            case "-list": list=Integer.parseInt(args[++i]); break;
            case "-buf": buf=Util.parseSize(args[++i]).intValue(); break;
            default: usage(); return;
         }
      }
      
      if(fixed){
         if(data<=0){
            if(keys>0) data=(40+900/list)*keys;
            else data=Integer.MAX_VALUE;
            System.out.println("data size forsed to: "+data);
         }
         if(data>Integer.MAX_VALUE){
            data=Integer.MAX_VALUE;
            System.out.println("data size exceeds limit, forsing to "+data);
         }
      }
      
      System.out.println("running with parameters:");
      System.out.println("  data="+data);
      System.out.println("  keys="+keys);
      System.out.println("  list="+list);
      System.out.println("  buf="+buf);
      System.out.println("  fixed="+fixed);
      
      run();
   }
   
   static void run(){
      DataMap map= fixed?
         new FixedDirectMap(20,8,6,list,(int)data).open() :
         new DirectMap(20,8,6,list,buf).open()
      ;
      DataMap.Cursor cursor=map.new Cursor();
      byte[] text=new byte[8];
      byte[] hash=new byte[20];
      
      long v=1;
      long start=System.currentTimeMillis();
      long t0=start;
      int c0=0;
      for(int c=1;;c++){
         if(data>0 && map.dataSize()>=(data-2000)) break;
         if(keys>0 && c>=keys) break;
         text[0]=(byte)v;
         text[1]=(byte)(v>>8);
         text[2]=(byte)(v>>16);
         text[3]=(byte)(v>>24);
         text[4]=(byte)(v>>32);
         text[5]=(byte)(v>>40);
         text[6]=(byte)(v>>48);
         text[7]=(byte)(v>>56);
         
System.arraycopy(text,0,hash,0,8);
//         SHA1Encoder.encode(text,hash);
         boolean b=cursor.put(hash,0);
         if(b) cursor.writeValue(0,0L,8);
         else cursor.writeValue(0,cursor.readValue(0,8)+1,8);
         
         long t1=System.currentTimeMillis();
         long T=(t1-start)/1000;
         long dt=t1-t0;
         if(dt>1000){
            System.out.println(
               "t="+T+" sec, keys="+c+
               ", map="+map.size()+
               ", data="+map.dataSize()+
               ", entry="+(map.dataSize()/map.size())+
               ", speed="+((c-c0)*1000.0/dt)+" keys/s"
            );
            c0=c;
            t0=t1;
         }
         
v*=131;
//         v*=3;
      }
      
      long[] dsum=new long[1],dnum=new long[1];
      cursor.scan(key->{
         dsum[0]+=cursor.depth;
         dnum[0]++;
      });
      
      System.out.println(
         "list size, avg entry size, avg depth: "+list+" , "+
         (map.dataSize()*1.0/map.size())+" , "+(dsum[0]*1.0/dnum[0])
      );
      map.dispose();
   }
   
   static void usage(){
      System.out.println(
         "args: [-fixed] [-data <max data size>] [-keys <max keys>] [-buf <value>]"
      );
   }
}
