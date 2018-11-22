package net.deepstorage.compscan.util;

import java.util.concurrent.atomic.*;

public class JobGroup{
   //private AtomicInteger num=new AtomicInteger(0);
   int num=0;
   private Runnable finalJob;
   
   public synchronized Runnable addJob(final Runnable job){
//System.out.println("JG.addJob()");
      num++;
      return new Runnable(){
         public void run(){
//System.out.println("JG.addJob().run(), num="+num);
            try{
               job.run();
            }
            finally{
               synchronized(JobGroup.this){
                  num--;
//System.out.println("  done, num="+num+", finalJob="+finalJob);
                  if(num==0){
                     if(finalJob!=null) finalJob.run();
                     JobGroup.this.notifyAll();
                  }
               }
            }
         }
      };
   }
   
   public synchronized void setFinalJob(Runnable job){
      if(num==0) job.run();
      finalJob=job;
   }
   
   public synchronized void waitAll(){
      while(num>0) try{
         wait();
      }
      catch(InterruptedException e){
         return;
      }
   }
}