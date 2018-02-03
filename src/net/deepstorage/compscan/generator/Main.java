package net.deepstorage.compscan.generator;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import net.deepstorage.compscan.Compressor;
import net.deepstorage.compscan.util.Util;

public class Main{
   private static String INDENT="  ";
   private static int LINE_LENGTH=100;
   
   static class ParameterHandler{
      String group;
      String name;
      String argsInfo;
      String descr;
      BiConsumer<Iterator<String>,App> parser;
   }
   
   static final Map<String,ParameterHandler> handlers=new HashMap<>();
   
   static void addHandler(
      String group, String name, String argsInfo, String descr, 
      BiConsumer<Iterator<String>,App> parser
   ){
      ParameterHandler h=new ParameterHandler();
      h.group=group;
      h.name=name;
      h.argsInfo=argsInfo;
      h.descr=descr;
      h.parser=parser;
      handlers.put(name,h);
   }
   
   static{
      addHandler("general", "help", "", "Prints help\n on usage",(x,y)->printHelp());
      addHandler(
         "output", "out", "<directory path>", "Output directory", (i,app)->{
            app.outputPath=i.next();
         }
      );
      addHandler(
         "output", "create", "", "Create output directory if missing", (i,app)->{
            app.createMissingDir=true;
         }
      );
      addHandler(
         "output", "overwrite", "", "Overwrite existing files", (i,app)->{
            app.overrideOutput=true;
         }
      );
      addHandler(
         "input", "hashes", "<path to file>", "Hashes statistics definition, compscan format",
         (i,app)->{
            app.hashesDefSupplier=new HashesDefFile(i.next());
         }
      );
      addHandler(
         "output", "numFiles", "<int number>", "Number of output files",
         (i,app)->{
            app.fileCount=Integer.parseInt(i.next());
         }
      );
      addHandler(
         "output", "prefix", "<string>", "Common prefix for output file names",
         (i,app)->{
            app.filenamePrefix=i.next();
         }
      );
      addHandler(
         "output", "suffix", "<int number>", "Start value for output filenames suffix",
         (i,app)->{
            app.indexOffset=Integer.parseInt(i.next());
         }
      );
      addHandler(
         "output", "suf_format", "<java decimal format string>", 
         "Suffix format, e.g. 00000 (5 digits, zero-padded)",
         (i,app)->{
            app.indexOffset=Integer.parseInt(i.next());
         }
      );
      addHandler(
         "output", "size", "<int number[k|m|g|t]>", 
         "Output file size, e.g. 1024, 1k, 500m. Used along with --dedup",
         (i,app)->{
            getHG(app).totalSize=Util.parseSize(i.next()).intValue();
         }
      );
      addHandler(
         "output", "dedup", "<float number, 0 to 1>", 
         "Desired deduplication ratio, used along with --size",
         (i,app)->{
            getHG(app).ratio=Float.parseFloat(i.next());
         }
      );
      addHandler(
         "output", "compressor", "<compressor name[:<option>]>",
         "Available compressor names: None, LZ4, GZIP, LZW\n"
            + "Options:\n"
            + "LZ4: compression level, number 0 - 17, default 9\n"
            + "GZIP: compression level, number 1 - 9, default 6\n"
         ,
         (i,app)->{
            String arg=i.next();
            try{
               app.compressor=Compressor.getCompressionInterface(arg);
            }
            catch(Exception e){
               throw new App.BadParam(e.getMessage());
            }
         }
      );
      addHandler(
         "output", "compression", "<float number, 0 to 1>", 
         "Desired compression ratio",
         (i,app)->{
            app.compressionRatio=Float.parseFloat(i.next());
         }
      );
      addHandler(
         "output", "blockSize", "<int number[k|m|g|t]>", 
         "Deduplication block size, e.g. 1024, 1k",
         (i,app)->{
            app.blockSize=Util.parseSize(i.next()).intValue();
            if(app.superblockSize==null) app.superblockSize=app.blockSize;
         }
      );
      addHandler(
         "output", "superblockSize", "<int number[k|m|g|t]>", 
         "Compression block size, e.g. 1024, 1k",
         (i,app)->{
            app.superblockSize=Util.parseSize(i.next()).intValue();
         }
      );
   }
   
   private static String lastLine;
   
   public static void main(String[] args) throws Exception{
      Iterator<String> argi=Arrays.asList(args).iterator();
      App app=new App();
      while(argi.hasNext()){
         String name=argi.next();
         if(!name.startsWith("--")){
            println("Error - unrecognized argument: "+name);
            printHelp();
            return;
         }
         name=name.substring(2);
         ParameterHandler handler=handlers.get(name);
         if(handler==null){
            println("Error - unrecognized parameter: "+name);
            printHelp();
            return;
         }
         handler.parser.accept(argi, app);
      }

      App.Stage stage=app.start();
      while(stage!=null) try{
         println(stage.name());
         while(stage.hasMoreSteps()){
            stage.step();
            if(stage.messages.size()>0){
               for(String msg: stage.messages) println(msg);
               stage.messages.clear();
            }
            clearln();
            print(((int)(stage.progress()*100))+"% done");
         }
         clearln();
         stage=stage.nextStage();
      }
      catch(App.BadParam err){
         println("Error in configuration: "+err.getMessage());
         printHelp();
         return;
      }
      app.close();
      println();
      println("done");
   }
   
   static void print(String s){
      lastLine= lastLine==null? s: lastLine+s;
      System.out.print(s);
   }
   static void println(String s){
      System.out.println(s);
      lastLine=null;
   }
   static void println(){
      System.out.println();
      lastLine=null;
   }
   static void clearln(){
      if(lastLine!=null) for(int i=lastLine.length(); i-->0;) System.out.print('\b');
      lastLine=null;
   }
   
   static void printHelp(){
      println("Usage:");
      println("java -cp <this jar path> net.deepstorage.compscan.generator.Main <parameters>");
      println("Parameters:");
      Map<String,List<ParameterHandler>> groupedHandlers=
         handlers.values().stream().collect(Collectors.groupingBy(h->h.group))
      ;
      groupedHandlers.entrySet().forEach(e->{
         String group=e.getKey();
         List<ParameterHandler> params=e.getValue();
         println(INDENT+group+":");
         params.forEach(p->{
            println(INDENT+INDENT+"--"+p.name+" "+ifNull(p.argsInfo, ""));
            formatLines(p.descr, LINE_LENGTH-5*INDENT.length()).forEach(line->{
               System.out.println(INDENT+INDENT+INDENT+INDENT+INDENT+line);
            });
         });
      });
   }
   
   public static List<String> formatLines(String s, int maxLen){
      List<String> result=new ArrayList<>();
      StringBuilder buffer=new StringBuilder();
      for(String token: s.split("(?m) +|^|$")){
         if(token.indexOf('\n')>=0){
            result.add(buffer.toString());
            buffer.setLength(0);
            continue;
         }
         if(buffer.length()+token.length()+1>maxLen){
            result.add(buffer.toString());
            buffer.setLength(0);
         }
         if(buffer.length()>0) buffer.append(" ");
         buffer.append(token);
      }
      if(buffer.length()>0) result.add(buffer.toString());
      return result;
   }
   
   private static <T> T ifNull(T value, T defaultValue){
      return value==null? defaultValue: value;
   }
   
   private static HashesDefGenerator getHG(App app){
      if(app.hashesDefSupplier instanceof HashesDefGenerator){
         return (HashesDefGenerator)app.hashesDefSupplier;
      }
      HashesDefGenerator hg=new HashesDefGenerator(app);
      app.hashesDefSupplier=hg;
      return hg;
   }
}
