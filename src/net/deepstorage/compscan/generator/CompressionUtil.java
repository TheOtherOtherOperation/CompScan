package net.deepstorage.compscan.generator;

import java.util.*;

import net.deepstorage.compscan.util.Util;
import net.deepstorage.compscan.CompressionInterface;
import net.deepstorage.compscan.compress.*;

/**
 * Generate byte block with certain compression ratio and sequences of such blocks
 **/

public class CompressionUtil{
   public static byte[] generate(CompressionInterface compressor, float ratio, int size){
      final byte[] randomData=randomData(size);
      final byte[] data=new byte[size];
      final byte[] zeros=new byte[size];
      final int M=(int)Math.round(size*ratio); //desired compressed length
      final int N=size/4; //number of full 4-byte words
      final int[] randomIndex=randomIndex(N);
      int n1=0; //number of zero words, lower bound
      int m1=compressor.compress(randomData,-1).length; //compressed size for low bound
      int n2=N; //number of zero words, upper bound
      int m2=compressor.compress(zeros,-1).length; //compressed size for upper bound
      
      if(m2>m1) throw new IllegalArgumentException("Bad compressor");
      if(M>=m1) return randomData;
      if(M<=m2) return zeros;
      while((n2-n1)>=4){
         int n=(n2+n1)>>1;
         copyAndEraze(randomData,n,data,randomIndex);
         int m=compressor.compress(data,-1).length;
         if(m==M) return data;
         if(m>M){
            n1=n;
            m1=m;
         }
         else{
            n2=n;
            m2=m;
         }
      }
      int d1=Math.abs(m1-M);
      int d2=Math.abs(m2-M);
      int bestN= d1<d2? n1: n2;
      int bestD= d1<d2? d1: d2;
      for(int n=n1+1; n<n2; n++){
         copyAndEraze(randomData,n,data,randomIndex);
         int d=Math.abs(compressor.compress(data,-1).length-M);
         if(d<bestD){
            bestN=n;
            bestD=d;
         }
      }
      copyAndEraze(randomData,bestN,data,randomIndex);
      return data;
   }
   
   //copy data, eraze nZeros words
   private static void copyAndEraze(byte[] src, int nZeros, byte[] dst, int[] index){
      int N=src.length/4;
      if(nZeros < N/2){
         System.arraycopy(src,0,dst,0,src.length);
         for(int c=0;c<nZeros;c++){
            int i=index[c]<<2;
            dst[i]=0;
            dst[i+1]=0;
            dst[i+2]=0;
            dst[i+3]=0;
         }
      }
      else{
         Arrays.fill(dst,(byte)0);
         for(int c=nZeros; c<N; c++){
            int i=index[c]<<2;
            System.arraycopy(src,i,dst,i,4);
         }
      }
   }
   
   private static byte[] randomData(final int size){
      Random rnd=new Random();
      byte[] data=new byte[size];
      int N=size/4;
      for(int c=0;c<N;c++){
         int v=rnd.nextInt();
         int i=c<<2;
         data[i]=(byte)v;
         data[i+1]=(byte)(v>>8);
         data[i+2]=(byte)(v>>16);
         data[i+3]=(byte)(v>>24);
      }
      return data;
   }
   
   private static int[] randomIndex(final int size){
      Random rnd=new Random();
      int[] index=new int[size];
      for(int i=0;i<size;i++) index[i]=i;
      for(int i=size-1;i>0;i--) swap(index, i, rnd.nextInt(i));
      return index;
   }
   
   private static void swap(int[] data, int i1, int i2){
      int tmp=data[i1];
      data[i1]=data[i2];
      data[i2]=tmp;
   }
   
   public static void main(String[] args){
//      int size=40;
//      int[] index=randomIndex(size);
//      int[] data=new int[size];
//      for(int i=0;i<size;i++) data[index[i]]=1;
//      System.out.println(Util.toString(index));
//      System.out.println(Util.toString(data));
//      System.out.println(Util.toString(randomData(size)));
      
//      CompressionInterface cpr=new GZIP();
//      final int totalSize=1<<20;
//      byte[] data=generate(cpr, 0.5f, totalSize);
//      int totalSize1=cpr.compress(data,-1).length;
//      System.out.println(totalSize+" -> "+totalSize1+", ration: "+(1f*totalSize1/totalSize));
//      
//      int size=4*1024;
//      System.out.println("chunks("+size+"):");
//      float avgRate=0;
//      float n=0;
//      for(int i=0; i<totalSize; i+=size){
//         int size1=cpr.compress(Arrays.copyOfRange(data, i, i+size),-1).length;
//         float rate=1f*size1/size;
//         avgRate+=rate;
//         n++;
//         System.out.println("  "+size1+" ("+rate+")");
//      }
//      avgRate/=n;
//      System.out.println("avgRate="+avgRate);
      
//      //compression of shifted blocks
//      CompressionInterface cpr=new GZIP();
//      final int size=10*1024;
//      final int nBlocks=10;
//      CyclicDataPool pool=new CyclicDataPool();
//      for(int i=nBlocks; i-->0;) pool.add(generate(cpr, 0.5f, size));
//      pool.skip(size/2);
//      float avgR=0;
//      for(int i=nBlocks; i-->0;){
//         int m=cpr.compress(pool.read(size),-1).length;
//         float r=1f*m/size;
//         avgR+=r;
//         System.out.println(i+": "+r);
//      }
//      avgR/=nBlocks;
//      System.out.println("avgR: "+avgR);
      
//      //compression of repeated chunks
//      CompressionInterface cpr=new GZIP();
//      int L=4*1024;
//      byte[] rnd=randomData(L);
//      CyclicDataPool pool=new CyclicDataPool();
//      pool.add(rnd);
//      pool.add(rnd);
//      pool.add(rnd);
//      pool.add(rnd);
//      
//      for(float f:new float[]{1.5f, 2f, 2.5f, 3f, 3.5f}){
//         int m=(int)(L*f);
//         System.out.println(f+": "+1f*cpr.compress(pool.read(m),-1).length/m);
//      }
   }
}
