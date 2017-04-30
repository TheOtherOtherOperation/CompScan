package net.deepstorage.compscan.util;

import java.io.File;
import net.deepstorage.compscan.util.map.FsMap;

public class FsMapSupplier extends DirectMapSupplier{
   private final File dir;
   
   public FsMapSupplier(
      int mdSize, int listSize, int logChunkSize, long maxDataSize, File dir
   ){
      super(mdSize, listSize, logChunkSize, maxDataSize);
      this.dir=dir;
   }
   
   public MdMap get(){
      return get(new FsMap(mdSize, 8, addrSize, listSize, logChunkSize,dir));
   }
   
   public String toString(){
      return super.toString()+", dir="+dir;
   }
}