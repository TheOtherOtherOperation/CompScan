/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import net.deepstorage.compscan.util.*;

/**
 * A helper class for generating SHA-1 digests.
 * 
 * Notice: this algorithm was taken directly from example code on anyexample.com. See below.
 * 
 * @see http://www.anyexample.com/programming/java/java_simple_class_to_compute_sha_1_hash.xml
 */
public class SHA1Encoder {
   public static final int MD_SIZE=20;
   
	static{
      try{
         SystemUtil.loadLibrary("compscan");
      }
      catch(Exception e){
         throw new Error(e);
      }
   }
   
   public static Object encode(byte[] in) throws Exception{
      byte[] md=new byte[MD_SIZE];
      encode(in,md);
      return new MD(md);
   }
   
   public static class MD{
      private final int hashCode;
      private final byte[] data;
      
      MD(byte[] array){
         int h=0;
         for(int i=array.length; i-->0;){
//            h=h*25147+array[i]*15913;
            h=h*31+array[i];
         }
         hashCode=h;
         data=array;
      }
      
      @Override
      public int hashCode(){
         return hashCode;
      }
      
      public boolean equals(Object o){
         if(o==null) return false;
         if(o==this) return true;
         if(o instanceof MD){
            MD that=(MD)o;
            if(hashCode!=that.hashCode) return false;
            byte[] d1=data;
            byte[] d2=that.data;
            if(d1==d2) return true;
            if(d1.length!=d2.length) return false;
            for(int i=d1.length;i-->0;){
               if(d1[i]!=d2[i]) return false;
            }
            return true;
         }
         return false;
      }
      
      public String toString(){
         return convertToHex(data);
      }
   }
   
   private static String convertToHex(byte[] data) {
      StringBuffer buf = new StringBuffer();
      for (int i = 0; i < data.length; i++) { 
         int halfbyte = (data[i] >>> 4) & 0x0F;
         int two_halfs = 0;
         do { 
             if ((0 <= halfbyte) && (halfbyte <= 9)) 
                 buf.append((char) ('0' + halfbyte));
             else 
                 buf.append((char) ('a' + (halfbyte - 10)));
             halfbyte = data[i] & 0x0F;
         } while(two_halfs++ < 1);
      } 
      return buf.toString();
   }
   
   public static native void encode(byte[] in, byte[] out);
   
//   public static byte[] encode(byte[] input) throws NoSuchAlgorithmException, UnsupportedEncodingException  { 
//       MessageDigest md;
//       md = MessageDigest.getInstance("SHA-1");
//       md.update(input, 0, input.length);
//       return md.digest();
//   }
   
   public static void main(String[] args) throws Exception{
      byte[] data=new byte[1000];
      //for(int i=1;i<data.length;i++) data[i]=(byte)(i+data[i-1]*37);
      
      byte[] md=new byte[MD_SIZE];
      encode(data,md);
      System.out.println(convertToHex(md));
      
      Object o1=encode(data);
      data[0]=1;
      Object o2=encode(data);
      System.out.println(o1+": "+o1.hashCode());
      System.out.println(o2+": "+o2.hashCode());
      System.out.println(o1.equals(o2));
      
//      MessageDigest mdi = MessageDigest.getInstance("SHA-1");
//      double cps=net.deepstorage.compscan.compress.SpeedTest.run(()->{
//         mdi.update(data, 0, data.length);
//         mdi.digest();
//         mdi.reset();
//      });
      
      double cps=net.deepstorage.compscan.compress.SpeedTest.run(()->{
         encode(data,md);
      });
      
      System.out.println("troughput: "+(cps*data.length));
   }
}
