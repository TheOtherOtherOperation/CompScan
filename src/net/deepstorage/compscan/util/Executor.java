package net.deepstorage.compscan.util;

import java.util.concurrent.*;
import java.io.*;

public class Executor{
   public static final int DEFAULT_POOL_SIZE;
   static{
      int tmp; //compiler bug
      try{
         tmp=SystemUtil.getCoreThreadCount();
      }
      catch(Exception e){
         tmp=2;
      }
      DEFAULT_POOL_SIZE=2;
   }
   
   private static final BlockingQueue<Runnable> queue=new ArrayBlockingQueue<Runnable>(2*DEFAULT_POOL_SIZE);
   
   private static final ThreadPoolExecutor executor=new ThreadPoolExecutor(
      0, DEFAULT_POOL_SIZE, 5, TimeUnit.MINUTES, queue,
      new RejectedExecutionHandler(){
         public void rejectedExecution(Runnable r, ThreadPoolExecutor executor){
            try{
               queue.put(r);
            }
            catch(InterruptedException e){
               throw new RejectedExecutionException();
            }
         }
      }
   );
   
   public static void exec(Runnable r){
      executor.execute(r);
   }
   
   public static void shutdown(){
      executor.shutdown();
   }
   
   public void setPoolSize(int s){
      executor.setMaximumPoolSize(s);
   }
   
   public static void main(String[] args) throws Exception{
      
   }
}