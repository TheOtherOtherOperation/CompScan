package net.deepstorage.compscan;

import java.util.*;
import java.nio.file.*;

public enum ScanMode{
   NORMAL,
   BIG{
      @Override
      Object parseArg(Iterator<String> data){
         if(!data.hasNext()) throw new IllegalArgumentException(
            "BIG mode requires following size specifier"
         );
         return parseFileSize(data.next());
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
   
   private static Long parseFileSize(String spec){
      if(spec==null || spec.length()==0) return null;
      char lastChar=spec.charAt(spec.length()-1);
      long scale=1;
      if(Character.isLetter(lastChar)){
         spec=spec.substring(0,spec.length()-1);
         switch(Character.toLowerCase(lastChar)){
            case 'k': scale=1<<10; break;
            case 'm': scale=1<<20; break;
            case 'g': scale=1<<30; break;
         }
      }
      try{
         return Long.parseLong(spec)*scale;
      }
      catch(NumberFormatException e){
         throw new IllegalArgumentException("Incorrect format for BIG size specifier: "+spec);
      }
   }
}