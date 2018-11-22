package net.deepstorage.compscan.generator;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.function.Supplier;

public class HashesDefFile implements Supplier<int[][]>{
   static final Pattern dataPattern=Pattern.compile("(\\d+)\\s*,\\s*(\\d+)");
   static final int G_BLOCKS=1;
   static final int G_REPEATS=2;
   
   static final String HEADERS_TEST="number of blocks,";
   
   private File file;
   
   public HashesDefFile(String path){
      file=new File(path);
      if(!file.exists() || !file.isFile()) throw new App.BadParam("Histogram file not found: "+path);
   }
   
   public int[][] get(){
      try{
         BufferedReader in=new BufferedReader(new FileReader(file));
         try{
            List<int[]> data=new ArrayList<>();
            String line=in.readLine(); //headers
            if(!line.startsWith(HEADERS_TEST)) throw new App.BadParam("Bad file format");
            while((line=in.readLine())!=null){
               Matcher m=dataPattern.matcher(line);
               if(m.find()){
                  data.add(new int[]{
                     Integer.parseInt(m.group(G_BLOCKS)),
                     Integer.parseInt(m.group(G_REPEATS))
                  });
               }
            }
            return data.toArray(new int[data.size()][]);
         }
         finally{
            in.close();
         }
      }
      catch(App.BadParam bp){
         throw bp;
      }
      catch(Exception e){
         throw new App.BadParam(e.getMessage());
      }
   }
   
   public static void main(String[] args) throws Exception{
      int[][] data=new HashesDefFile("../testdata/out/hashes.csv").get();
      for(int[] item:data){
         System.out.println(item[0]+", "+item[1]);
      }
   }
}
