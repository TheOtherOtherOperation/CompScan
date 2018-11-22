package net.deepstorage.compscan.util;

import java.io.File;
import net.deepstorage.compscan.util.map.FsMap;

public class FsMapSupplier extends DirectMapSupplier{
   private final File dir;
   
   public FsMapSupplier(
      int mdSize, int listSize, int chunkSize, long maxDataSize, File dir
   ){
      super(mdSize, listSize, chunkSize, maxDataSize);
      this.dir=dir;
   }
   
   public MdMap get(){
      return get(new FsMap(mdSize, 8, addrSize, listSize, chunkSize, dir));
   }
   
   public String toString(){
      return super.toString()+", dir="+dir;
   }
}