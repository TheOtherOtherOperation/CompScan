package net.deepstorage.compscan.generator;

import java.io.*;
import java.util.*;
import java.text.*;
import java.util.function.*;
import java.util.function.*;
import net.deepstorage.compscan.CompressionInterface;
import net.deepstorage.compscan.compress.*;
import net.deepstorage.compscan.util.Util;

public class App {
   public static class BadParam extends RuntimeException{
      public BadParam(String msg){
         super("Bad parameter: "+msg);
      }
   }
   
   public interface Stage{
      public String name();
      public boolean hasMoreSteps();
      public void step() throws Exception;
      public float progress();        //-1 if udefined
      public Stage nextStage();
      public List<String> messages=new ArrayList<>();
   }
   
   public Supplier<int[][]> hashesDefSupplier; //{{nBlocks,nRepeats},...}
   public CompressionInterface compressor;
   public Float compressionRatio;
   public Integer blockSize;
   public Integer superblockSize;
   
   public String outputPath;
   public Boolean createMissingDir=true;
   public Boolean overrideOutput=false;
   public String filenamePrefix;
   public Integer fileCount;
   public Integer indexOffset;
   public String fileIndexFormat;
   
   private File outputDir;
   private int currentFileIndex;
   private OutputStream currentOutput;
   private Generator currentGenerator;
   private int[][] hashesDef;
   
   public Stage start(){
      return new CheckParams();
   }
   
   public void close(){
      try{
         if(currentOutput!=null) currentOutput.close();
         currentOutput=null;
      }
      catch(Exception e){}
   }
   
   public void reset(){
      close();
      currentFileIndex=0;
      currentGenerator=null;
      hashesDef=null;
   }
   
   abstract class AbstractStage implements Stage{
      protected String name;
      protected float progress;
      protected boolean done=false;
      
      public String name(){
         return name;
      }
      public boolean hasMoreSteps(){
         return !done;
      }
      public void step() throws Exception{
         singleStep();
         done=true;
      }
      protected void singleStep() throws Exception{
         //implement
      }
      public float progress(){
         return done? 1f: progress;
      }
   }
   
   class CheckParams extends AbstractStage{
      CheckParams(){
         name="Checking parameters";
      }
      @Override
      public void singleStep() throws Exception{
         if(hashesDefSupplier == null) throw new BadParam("Data specification not defined");
         if(compressor == null) throw new BadParam("Compressor not defined");
         if(compressionRatio == null) throw new BadParam("Compression ratio not defined");
         if(blockSize == null) throw new BadParam("Block size not defined");
         if(superblockSize == null) superblockSize=blockSize;
         if(outputPath == null) throw new BadParam("Output directory not defined");
         if(filenamePrefix == null) throw new BadParam("File name prefix not defined");
         if(fileIndexFormat == null) fileIndexFormat="000000";
         if(fileCount == null) fileCount=1;
         if(indexOffset == null) indexOffset=0;
         if(createMissingDir == null) createMissingDir=true;
         if(overrideOutput == null) overrideOutput=false;
      }
      public Stage nextStage(){
         return new PrepareOutputDir();
      }
   }
   
   class PrepareOutputDir extends AbstractStage{
      PrepareOutputDir(){
         name="Preparing output directory";
      }
      @Override
      protected void singleStep() throws Exception{
         File dir=new File(outputPath);
         if(!dir.exists()){
            if(createMissingDir==Boolean.TRUE) dir.mkdirs();
            else throw new BadParam("Output path doesn't exist");
         }
         if(!dir.isDirectory()) throw new RuntimeException("Output path is not a drectory");
         String[] files=dir.list();
         if(files.length>0){
            if(overrideOutput==Boolean.TRUE) messages.add("WARNING: Output directory is not empty");
            else throw new BadParam("Output path is not empty");
         }
         outputDir=dir;
         messages.add("Output directory: "+dir);
      }
      
      public Stage nextStage(){
         return openFile();
      }
   }
   
   private Stage openFile(){
      if(currentFileIndex>=fileCount) return null;
      return new AbstractStage(){{
            name="Opening file #"+(currentFileIndex+1)+" of "+fileCount;
         }
         protected void singleStep() throws Exception{
            NumberFormat nf=new DecimalFormat(fileIndexFormat);
            String filename=filenamePrefix+nf.format(currentFileIndex);
            currentOutput=new FileOutputStream(new File(outputPath, filename));
            messages.add("Current file: "+filename);
         }
         public Stage nextStage(){
            return new BuildGenerator();
         }
      };
   }
   
   class BuildGenerator extends AbstractStage{
      BuildGenerator(){
         name="Preparing block generator";
         currentGenerator=new Generator(compressor, compressionRatio, blockSize, superblockSize);
         if(hashesDef==null) hashesDef=hashesDefSupplier.get();
      }
      int groupIndex;
      
      @Override
      public boolean hasMoreSteps(){
         return groupIndex<hashesDef.length;
      }
      public void step() throws Exception{
         currentGenerator.addGroup(hashesDef[groupIndex][0], hashesDef[groupIndex][1]);
         groupIndex++;
         progress=1f*groupIndex/hashesDef.length;
      }
      public Stage nextStage(){
         return new Generate();
      }
   }
   
   class Generate extends AbstractStage{{
         name="Generating file #"+(currentFileIndex+1);
         seq=currentGenerator.build();
      }
      Enumeration<byte[]> seq;
      int blocksGenerated;
      @Override
      public boolean hasMoreSteps(){
         return seq.hasMoreElements();
      }
      public void step() throws Exception{
         currentOutput.write(seq.nextElement());
         blocksGenerated++;
         progress=1f*blocksGenerated/currentGenerator.totalBlockCount();
      }
      public Stage nextStage(){
         close();
         currentFileIndex++;
         return openFile();
      }
   }
   
   private static void ack(String msg){
      System.out.println(msg);
      try{
         System.in.read();
      }
      catch(Exception e){}
   }
   
   public static void main(String[] args) throws Exception{
      App app=new App();
      app.hashesDefSupplier=()->new int[][]{
         {49,1},
         {1,51}
//         {1,5},
//         {5,1}
      };
      app.compressor=new GZIP(6);
      app.compressionRatio=0.5f;
      app.blockSize=4*1024;
      app.superblockSize=8*1024;
      
      app.outputPath="../tmp/test/CompScan-master/test/app"; //"D:/tmp/compscan/generated";
      app.createMissingDir=true;
      app.overrideOutput=true;
      app.filenamePrefix="app";
      app.fileCount=5;
      app.indexOffset=0;
      app.fileIndexFormat="00";
      
      Stage s=app.start();
      while(s!=null){
         System.out.println(s.name());
         while(s.hasMoreSteps()){
//            ack("next step");
            s.step();
            if(s.messages.size()>0){
               System.out.println("Messages: "+s.messages);
               s.messages.clear();
            }
            System.out.println(((int)(s.progress()*100))+"% done");
         }
//         ack("next stage");
         s=s.nextStage();
      }
      app.close();
      System.out.println("done");
   }
}
