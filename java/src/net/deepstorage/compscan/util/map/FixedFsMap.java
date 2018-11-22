package net.deepstorage.compscan.util.map;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import net.deepstorage.compscan.util.Util;

public class FixedFsMap extends FixedDirectMap{
   private final List<Object[]> files=new ArrayList<>();
   private final File dir;
   
   public FixedFsMap(
      int keySize, int valueSize, int addrSize,
      int maxListSize, int size, File dir
   ){
      super(keySize, valueSize, addrSize, maxListSize, size);
      this.dir=dir;
   }
   
   protected ByteBuffer createBuffer(int size){
      try{
         File f=File.createTempFile("compscan","map",dir);
         RandomAccessFile raf=new RandomAccessFile(f,"rw");
         try{
            ByteBuffer bb=raf.getChannel().map(FileChannel.MapMode.PRIVATE,0,size);
            files.add(new Object[]{f,raf});
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
   
   public void dispose(){
      super.dispose();
      for(Object[] data:files){
         File f=(File)data[0];
         RandomAccessFile raf=(RandomAccessFile)data[1];
         try{
            raf.close();
         }
         catch(Exception e){}
         f.delete();
      }
   }
}
