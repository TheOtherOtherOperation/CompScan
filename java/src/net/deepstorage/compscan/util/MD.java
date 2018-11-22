package net.deepstorage.compscan.util;

//represents message digest (a hash)
//just a wrapped byte array

public class MD{
   private final int hashCode;
   public final byte[] data;
   public final int off, len;
   
   public MD(byte[] data){
      this(data,0,data.length);
   }
   public MD(byte[] data, int off, int len){
      int h=0;
      for(int i=len; i-->0;){
         h=h*31+(data[off+i]&0xff);
      }
      hashCode=h;
      this.data=data;
      this.off=off;
      this.len=len;
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
         if(this.len!=that.len) return false;
         byte[] d1=data;
         byte[] d2=that.data;
         int off1=this.off;
         int off2=that.off;
         if(d1==d2 && off1==off2) return true;
         for(int i=len;i-->0;){
            if(d1[off1+i]!=d2[off2+i]) return false;
         }
         return true;
      }
      return false;
   }
   
   public String toString(){
      return Util.toHexString(data,off,len);
   }
}
