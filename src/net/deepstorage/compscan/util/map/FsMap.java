package net.deepstorage.compscan.util.map;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import net.deepstorage.compscan.util.Util;


public class FsMap extends DirectMap{
   private final HashMap<ByteBuffer,Object[]> files=new HashMap<>();
   private final File dir;
   
   public FsMap(
      int keySize, int valueSize, int addrSize,
      int maxListSize, int logBufSize, File dir
   ){
      super(keySize, valueSize, addrSize, maxListSize, logBufSize);
      this.dir=dir;
   }
   
   protected ByteBuffer createBuffer(int size){
      try{
         File f=File.createTempFile("compscan","map",dir);
         RandomAccessFile raf=new RandomAccessFile(f,"rw");
         try{
            ByteBuffer bb=raf.getChannel().map(FileChannel.MapMode.PRIVATE,0,size);
            files.put(bb,new Object[]{f,raf});
            return bb;
         }
         catch(Exception e){
            raf.close();
            throw e;
         }
      }
      catch(Exception e){
         throw new RuntimeException(e);
      }
   }
   
   protected void dispose(ByteBuffer bb){
      Object[] data=files.get(bb);
      if(data==null) return;
      File f=(File)data[0];
      RandomAccessFile raf=(RandomAccessFile)data[1];
      super.dispose(bb);
      try{
         raf.close();
      }
      catch(Exception e){}
      f.delete();
   }
}