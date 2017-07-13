package net.deepstorage.compscan.util;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;
import java.util.function.*;

/*
 * Read multiple files as one stream
 */

public class PathByteStream implements MultipartByteStream{
   private final Path root;
   private final Iterator<Path> files;
   private InputStream currentStream;
   private boolean eof;
   
   public PathByteStream(String rootPath, Predicate<Path> ... filters) throws IOException{
      this(Paths.get(rootPath), filters);
   }
   
   public PathByteStream(Path root, Predicate<Path> ... filters) throws IOException{
      this.root=root;
      Stream<Path> fs=Files.walk(root).filter(path->!Files.isDirectory(path));
      for(Predicate<Path> filter: filters) fs=fs.filter(filter);
      files=toIterator(fs);
   }
   
   //plug to path enumeration
   protected Iterator<Path> toIterator(Stream<Path> paths) throws IOException{
      return paths.iterator();
   }
   
   //read exactly len bytes, if available
   //on file boundary return bytes read so far
   //on end of data return bytes read so far or -1
   public int read(byte[] buf, int off, int len) throws IOException{
      if(currentStream==null && !nextStream()) return -1;
      int c=0;
      while(c<len){
         int n=currentStream.read(buf,off+c,len-c);
         if(n>=0){
            eof=false;
            c+=n;
         }
         else{
            eof=true;
            if(nextStream()) return c;
            return c==0? -1: c;
         }
      }
      return c;
   }
   
   public boolean isPartBoundary(){
      return eof;
   }
   
   public void close(){
      closeCurrent();
   }
   
   private boolean nextStream() throws IOException{
      closeCurrent();
      if(!files.hasNext()) return false;
      currentStream=new BufferedInputStream(Files.newInputStream(files.next()));
      return true;
   }
   
   private void closeCurrent(){
      if(currentStream!=null) try{
         currentStream.close();
         currentStream=null;
      }
      catch(Exception e){}
   }
   
   public static void main(String[] args) throws Exception{
      PathByteStream in=new PathByteStream("../testdata/in/alpha");
      byte[] buf=new byte[1000];
      int tc=0;
      int blocks=0;
      for(;;){
         int c=in.read(buf,0,2);
         System.out.println(" <- "+c);
         if(c>0) tc+=c;
         if(in.isPartBoundary()) System.out.println("eof! tc="+tc);
         if(c==-1) break;
//         if(blocks<1000){
//            String s= c<=30?
//               net.deepstorage.compscan.util.Util.toString(buf,0,c):
//               net.deepstorage.compscan.util.Util.toString(buf,0,30)+"..."
//            ;
//            System.out.println("block "+blocks+": "+c+":"+s);
//            blocks++;
//         }
      }
      System.out.println("==");
      System.out.println(tc);
   }
}
