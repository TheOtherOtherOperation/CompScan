package net.deepstorage.compscan.generator;

import java.util.function.Supplier;

public class HashesDefGenerator implements Supplier<int[][]>{
   private final App app;
   public Integer totalSize;
   public Float ratio;
   
   public HashesDefGenerator(App app){
      if(app==null) throw new IllegalArgumentException();
      this.app=app;
   }
   
   public int[][] get(){
      if(totalSize==null || totalSize<=0) throw new App.BadParam("Missing file size");
      if(ratio==null || ratio<0) throw new App.BadParam("Missing dedup ratio");
      if(app.blockSize==null || app.blockSize<=0) throw new App.BadParam("Missing block size");
      int N=totalSize/app.blockSize;
      int M=(int)(N*ratio);
      return new int[][]{
         {M-1, 1},
         {1, N-M+1}
      };
   }
}
