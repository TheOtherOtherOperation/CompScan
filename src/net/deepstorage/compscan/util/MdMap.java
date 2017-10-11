package net.deepstorage.compscan.util;

public interface MdMap{
   public interface Consumer{
      public void consume(byte[] md, int off, long count);
   }
   
   public int keyLength();
   public long add(byte[] key, int off, long v);
   public long get(byte[] key, int off);
   public int size();
   public void scan(Consumer feed);
   public void dispose();
}
