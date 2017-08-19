package net.deepstorage.compscan.util;

import java.util.*;
import java.text.*;
import java.io.*;
import java.util.stream.*;
import java.nio.file.*;

public class StreamTest{
   static NumberFormat fmt=new DecimalFormat("#.000");
   
   static long INTERVAL=1000;
   
   public static void main(String[] args) throws IOException{
if(args.length==0) args=new String[]{"../testdata/in/all"};
      MultipartByteStream in=new PathByteStream(args[0]){
         protected Iterator<Path> toIterator(Stream<Path> paths) throws IOException{
            return paths.map(path->{
               print("opened "+path);
               return path;
            }).iterator();
         }
      };
      byte[] buf=new byte[1<<20];
      long bytes=0, total=0;
      final long start=System.currentTimeMillis();
      long t0=start;
      while(!in.isEos()){
         int c=in.read(buf);
         bytes+=c;
         total+=c;
         long t1=System.currentTimeMillis();
         long dt=t1-t0;
         if(dt>=INTERVAL){
            print(
               "total="+fmt.format(total*1.0/(1<<20))+" MB, "+
               "speed="+fmt.format(bytes*1000.0/dt/(1<<20))+" MB/s"
            );
            bytes=0;
            t0=t1;
         }
      }
      print("total="+fmt.format(total*1.0/(1<<20))+" MB");
      final long end=System.currentTimeMillis();
      print("total speed="+fmt.format(total*1.0/(1<<20)*1000/(end-start))+" MB/s");
      
   }
   
   static void print(String s){
      System.out.println(s);
   }
}
