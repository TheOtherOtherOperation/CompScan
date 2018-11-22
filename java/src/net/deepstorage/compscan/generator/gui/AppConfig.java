package net.deepstorage.compscan.generator.gui;

import java.io.*;
import java.util.*;

import javax.swing.UIManager;

public class AppConfig{
   private static final String CFG_FILE="app.cfg";
   
   public static final FilenameFilter CSV_FILES=new FilenameFilter(){
      public boolean accept(File dir, String name){
         return name.endsWith(".csv");
      }
   };
   
   private static Map map;
   private static final Object[][] defaultValues=new Object[][]{
      //{name, value},
   };
   
   //most used parameters
   public static final String WORKING_DIR="WORKING_DIR";
   
   public static void put(String key, Serializable value){
      map.put(key,value);
   }
   
   public static void put(String key, int i){
      int[] ref=(int[])map.get(key);
      if(ref==null) map.put(key, new int[]{i});
      else ref[0]=i;
   }
   
   public static void put(String key, double d){
      double[] ref=(double[])map.get(key);
      if(ref==null) map.put(key, new double[]{d});
      else ref[0]=d;
   }
   
   public static Object get(String key){
      return map.get(key);
   }
   
   public static Object get(String key, Serializable deft){
      Object o=map.get(key);
      if(o==null) map.put(key, o=deft);
      return o;
   }
   
   public static String getString(String key){
      return (String)get(key);
   }
   
   public static String getString(String key, String defaultValue){
      String s=(String)get(key);
      return s==null? defaultValue: s;
   }
   
   public static int getInt(String key){
      return ((int[])get(key))[0];
   }
   
   public static double getDouble(String key){
      return ((double[])get(key))[0];
   }
   
   public static double getDouble(String key, double deft){
      double[] ref=((double[])get(key));
      if(ref==null) return deft;
      return ref[0];
   }
   
   static{
      try{
         InputStream in=new FileInputStream(CFG_FILE);
         try{
            map=(Map)new ObjectInputStream(in).readObject();
         }
         finally{
            in.close();
         }
      }
      catch(Exception e){
         map=new HashMap();
      }
      for(int i=0;i<defaultValues.length;i++){
         if(!map.containsKey(defaultValues[i][0])){
            map.put(defaultValues[i][0], defaultValues[i][1]);
         }
      }
      Runtime.getRuntime().addShutdownHook(new Thread(){
         public void run(){
            try{
               OutputStream out=new FileOutputStream(CFG_FILE);
               try{
                  new ObjectOutputStream(out).writeObject(map);
               }
               finally{
                  out.close();
               }
            }
            catch(Exception e){
               e.printStackTrace();
            }
         }
      });
      //UIManager.put("OptionPane.yesButtonText", STR_YES);
      //UIManager.put("OptionPane.noButtonText", STR_NO);
   }
   
   public static void main(String[] args)throws Exception{
      //
   }
}