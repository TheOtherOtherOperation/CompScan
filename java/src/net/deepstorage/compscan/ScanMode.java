package net.deepstorage.compscan;

import java.util.*;
import java.nio.file.*;
import net.deepstorage.compscan.util.Util;

public enum ScanMode{
   NORMAL,
   BIG{
      @Override
      Object parseArg(Iterator<String> data){
         if(!data.hasNext()) throw new IllegalArgumentException(
            "BIG mode requires following size specifier"
         );
         return Util.parseSize(data.next());
      }
      @Override
      void run(CompScan host){
         long minSize=((Number)host.scanModeArg).longValue();
         host.runFiles(f -> Files.isRegularFile(f) && f.toFile().length()>=minSize);
      }
   },
   VMDK{
      @Override
      void run(CompScan host){
         host.runFiles(f -> isVmdk(f));
      }
   };
   
   //vmdk file extensions.
   static final List<String> VMDK_EXTENSIONS=Arrays.asList(new String[]{
      "txt", "vhd", "vhdx", "vmdk"
   });
   
   Object parseArg(Iterator<String> data){
      return null;
   }
   void run(CompScan host){
      host.run();
   };
   
   private static boolean isVmdk(Path path) {
      if(path == null) return false;
      String ext=extension(path);
      return 
         ext!=null &&
         Files.isRegularFile(path) && 
         VMDK_EXTENSIONS.contains(ext.toLowerCase())
      ;
   }
   
   private static String extension(Path f){
      String[] parts = f.getFileName().toString().split("\\.(?=\\w+$)");
      if(parts.length<2) return null;
      return parts[parts.length-1];
   }
}