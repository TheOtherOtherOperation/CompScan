package net.deepstorage.compscan.util;

import java.io.*;
import java.net.URL;

public class SystemUtil{
   public static final String WINDOWS="Windows";
   public static final String LINUX="Linux";
   public static final String OS=detectOs();
   public static final String ARCH=detectArch();
   public static final String EXT= OS.equals(WINDOWS)? "dll": "so";
   
   //name without ext
   public static void loadLibrary(String name) throws IOException{
      String fullName=name+"."+EXT;
      File dir=new File(System.getProperty("user.dir")+"/.compscan");
      if(!dir.exists()) dir.mkdir();
      File file=new File(dir,fullName);
      if(!file.exists()){
         String rpath="/lib/"+OS+"/"+ARCH+"/"+fullName;
         URL res=SystemUtil.class.getResource(rpath);
         if(res==null) throw new RuntimeException("dll resource not found: "+rpath);
         InputStream in=res.openConnection().getInputStream();
         try{
            OutputStream out=new FileOutputStream(file);
            try{
               byte[] buf=new byte[256];
               for(int c; (c=in.read(buf))>=0; ) out.write(buf,0,c);
            }
            finally{
               out.close();
            }
         }
         finally{
            in.close();
         }
      }
      System.load(file.getPath());
   }
   
   private static String detectOs(){
      String s=System.getProperty("os.name").split(" ")[0];
      if(s.startsWith("GNU/")) s=s.substring(4);
      return s;
   }
   
   //JVM arch
   private static String detectArch(){
      String dm=System.getProperty("sun.arch.data.model");
      if(OS.equals("Windows")){
         //we assume that windows is always x86
         if(dm.equals("32")) return "x86";
         if(dm.equals("64")) return "x86_64";
      }
      String arch=System.getProperty("os.arch");
      if(
         arch.matches("x86.*")  || 
         arch.matches("i\\d+")  || 
         arch.matches("ia\\d+") ||
         arch.matches("amd\\d+")
      ){
         if(dm.equals("32")) return "x86";
         if(dm.equals("64")) return "x86_64";
      }
      return arch;
   }
   
   public static int getCoreThreadCount() throws Exception{
      if(OS.equals(LINUX)) try{ // more precise with respect to hyperthreads
         return getCoreThreadCountLinux();
      }
      catch(Exception e){}
      return Runtime.getRuntime().availableProcessors();
   }
   
   public static int getCoreThreadCountLinux() throws Exception{
      BufferedReader localBufferedReader = new BufferedReader(new FileReader("/proc/cpuinfo"));
      int i = 0;
      for(;;){
         String s=localBufferedReader.readLine();
         if(s==null) break;
         if(!s.startsWith("flags")) continue;
         i++;
         if(s.matches(".*\\bht\\b.*")) i++;
      }
      if(i==0) throw new Exception("unexpected contents of /proc/cpuinfo");
      return i;
   }
   
   public static void main(String[] args) throws Exception{
      System.out.println(OS);
      System.out.println(ARCH);
      System.out.println(getCoreThreadCount());
      loadLibrary("compscan");
   }
}