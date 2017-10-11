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
   private boolean eof, eos;
   
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
   
   //read 0...len bytes, if available, return their number
   //(may be less than requested)
   //don't read past file boundaries
   public int read(byte[] buf, int off, int len) throws IOException{
      if(currentStream==null){
         if(eos) throw new IOException("reading past EOS");
         if(!nextStream()) return 0;
      }
      int c=currentStream.read(buf,off,len);
      if(c==-1){
         nextStream();
         eof=true;
         return 0;
      }
      eof=false;
      return c;
   }
   
   public boolean isEos(){
      return eos;
   }
   
   public boolean isPartBoundary(){
      return eof;
   }
   
   public void close(){
      closeCurrent();
   }
   
   private boolean nextStream() throws IOException{
      closeCurrent();
      if(!files.hasNext()){
         eos=true;
         return false;
      }
      currentStream=Files.newInputStream(files.next());
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
      //PathByteStream in=new PathByteStream("../testdata/in/tiny");
      PathByteStream in=new PathByteStream("../testdata/in/small");
      byte[] buf=new byte[1000];
      int tc=0;
      int blocks=0;
      for(;;){
         //int c=in.read(buf,0,2);
         int c=in.read(buf);
         System.out.println(" <- "+c);
         tc+=c;
         if(in.isPartBoundary()) System.out.println("<file boundary at "+tc+">");
         if(in.isEos()) break;
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
