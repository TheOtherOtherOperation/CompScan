package net.deepstorage.compscan.generator;

import java.util.Enumeration;
import java.util.function.Supplier;

interface BlockOrder {
   /*
    * @param blocks number of unique blocks in the batch
    * @param repeats number of repeats for each unique block
    * @param data - supplies properly compressible blocks; no restriction here.
    * The implementation must enforce block uniqueness by adding proper mark
    * to the end of block
    */
   public void addGroup(int blocks, int repeats, Supplier<byte[]> data);
   
   public Enumeration<byte[]> toBlockSequence();
   
   public int getBlockCount();
   public int getUniqueBlockCount();
}
