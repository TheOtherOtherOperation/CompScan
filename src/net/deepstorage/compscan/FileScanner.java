/**
 * 
 */
package net.deepstorage.compscan;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import net.deepstorage.compscan.CompScan.MutableCounter;
import net.deepstorage.compscan.CompScan.Results;
import net.deepstorage.compscan.Compressor.BufferLengthException;
import net.deepstorage.compscan.Compressor.CompressionInfo;

import net.deepstorage.compscan.util.*;

/**
 * The FileScanner class abstracts the necessary behavior for walking a file tree.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class FileScanner {
	private final Path root;
   private final int[] blockSizes;
   private final int superblockSize;
   private final int bufferSize;
   private final Results[] totals;
   private final AtomicLong[] hashCounters;
   private final Compressor compressor;
   private final Double ioRate;
   private final boolean verbose;
	
	/**
    * bufferSize - processing chunk
    */
	public FileScanner(
      Path root, int[] blockSizes, int superblockSize, int bufferSize, Results[] totals,
      AtomicLong[] hashCounters, Compressor compressor, Double ioRate, boolean verbose
   ){
      if(blockSizes.length!=totals.length) throw new IllegalArgumentException(blockSizes.length+"!="+totals.length);
      if(hashCounters.length!=blockSizes.length) throw new IllegalArgumentException(hashCounters.length+"!="+blockSizes.length);
      this.root = root;
      this.blockSizes = blockSizes;
      this.superblockSize=superblockSize;
      this.bufferSize= bufferSize;
      this.totals= totals;
      this.hashCounters= hashCounters;
      this.compressor = compressor;
      this.verbose = verbose;
      this.ioRate = ioRate;
	}
	
	/**
	 * Run scan.
	 * 
	 * @throws IOException if an IO error occurs.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 * @throws NoNextFileException if file root contains no regular files.
	 */
	public void scanCombined() throws IOException{
      MultipartByteStream pathStream=new PathByteStream(root){
         protected Iterator<Path> toIterator(Stream<Path> paths) throws IOException{
            int[] pathCount=new int[1];
            paths=paths.map(path->{
               pathCount[0]++;
               for(Results r: totals) r.set("files read", pathCount[0]);
               return path;
            });
            if(verbose) paths=paths.map(path->{
               System.out.println("Opening file: \"" + path + "\"");
               return path;
            });
            Iterator<Path> i=paths.iterator();
            if(!i.hasNext()) throw new IOException(String.format(
               "FileWalkerStream with root \"%s\" contains no scannable data.", root
            ));
            return i;
         }
      };
      MultipartByteStream src=pathStream;
      if(ioRate!=null){
         src=new MultipartByteStream.Adapter(new TrottleByteStream(src,ioRate)){
            @Override
            public boolean isPartBoundary(){
               return pathStream.isPartBoundary();
            }
         };
      }
      scanStream(src, null);
   }
   
   private void scanStream(MultipartByteStream src, Results[] results) throws IOException{
      if(results!=null && results.length!=totals.length) throw new IllegalArgumentException(results.length+"!="+totals.length);
      final int chunkSize=bufferSize;
      final int bufferSize=chunkSize+Math.max(superblockSize, Util.max(blockSizes));
      byte[] prevBuffer=null;
      int prevSize=0;
      int[] remainders=new int[blockSizes.length+1];
      JobGroup jg=new JobGroup();
      for(;;){
         byte[] buffer=new byte[bufferSize];
         int c=src.read(buffer,0,chunkSize);
//System.out.println("  read "+c+" bytes");
         scheduleBuffer(
            buffer,c,prevBuffer,prevSize,remainders,src.isPartBoundary(),results,jg
         );
//System.out.println("    scheduled");
         if(c==-1) break;//eos
         prevBuffer=buffer;
         prevSize=c;
      }
//System.out.println("  done");
      jg.waitAll();
   }

static int blockCount=0;   
   //asyncronously scan supreblock + remainders from prev supreblock scan
   private void scheduleBuffer(
      byte[] buffer, final int size, byte[] prevBuffer, final int prevSize,
      int[] remainders, boolean isFileBoundary, Results[] results, JobGroup jg
   ){
//System.out.println("FS.scheduleBuffer(["+buffer+"],"+size+","+prevBuffer+","+Util.toString(remainders)+",...)");
      final int[] remaindersCopy=(int[])remainders.clone();
      final int superblockInd=blockSizes.length;
      for(int i=0;i<blockSizes.length;i++){
         remainders[i]= isFileBoundary? 0: (remainders[i]+size)%blockSizes[i];
      }
      remainders[superblockInd]=
         isFileBoundary? 0: (remainders[superblockInd]+size)%superblockSize
      ;
//System.out.println("  remainders -> "+Util.toString(remainders));
      Executor.exec(jg.addJob(()->{
//System.out.println("  remaindersCopy="+Util.toString(remaindersCopy));
         for(int i=0;i<blockSizes.length;i++){
            final int rem=remaindersCopy[i];
            final int blockSize=blockSizes[i];
            List<byte[]> hashes=new ArrayList<>(20);
//System.out.println("  scan hashes in "+buffer.length+":"+Util.toString(buffer,0,20)+", prev="+prevBuffer+", rem="+rem+", blockSize="+blockSize);
            scanBlocks(
               buffer,size,prevBuffer,prevSize,rem,blockSize,isFileBoundary,
               block->{
//byte[] hash=SHA1Encoder.encode(block);
//System.out.println("  block "+(blockCount++)+": "+Util.toString(block,0,20)+" -> "+Util.toHexString(hash));
//hashes.add(hash);
                  hashes.add(SHA1Encoder.encode(block));
               }
            );
            if(results!=null) results[i].updateHashes(hashes);
            totals[i].updateHashes(hashes);
            hashCounters[i].set(
               results!=null? results[i].getHashes().size():
               totals[i].getHashes().size()
            );
         }
         int rem=remaindersCopy[superblockInd];
         scanBlocks(
            buffer,size,prevBuffer,prevSize,rem,superblockSize,isFileBoundary,
            block->{
               Function<Integer,CompressionInfo> f=compressor.process(block);
               for(int i=0;i<blockSizes.length;i++){
                  CompressionInfo ci=f.apply(blockSizes[i]);
                  if(results!=null) results[i].feedCompressionInfo(ci);
                  totals[i].feedCompressionInfo(ci);
               }
            }
         );
      }));
   }
   
   private static void scanBlocks(
      byte[] buffer, int size, byte[] prevBuffer, final int prevSize,
      final int remainder, final int blockSize, boolean isFileBoundary,
      Consumer<byte[]> op
   ){
//System.out.println("scanBlocks("+buffer.length+":"+Util.toString(buffer,0,20)+","+size+","+prevBuffer+","+remainder+","+blockSize+",..)");
      final byte[] block=new byte[blockSize];
      int off=0;
      if(remainder>0){
         System.arraycopy(prevBuffer, prevBuffer.length-remainder, block, 0, remainder);
         System.arraycopy(buffer, 0, block, remainder, blockSize-remainder);
         op.accept(block);
         off=blockSize-remainder;
      }
      while(size>=blockSize){
         System.arraycopy(buffer, off, block, 0, blockSize);
         op.accept(block);
         off+=blockSize;
         size-=blockSize;
      }
      if(isFileBoundary && size>0){
         System.arraycopy(buffer, off, block, 0, size);
         for(int i=size;i<blockSize;i++) block[i]=0;
         op.accept(block);
      }
   }
	
   public void scanSeparately(
      List<Results[]> fileResults, CompScan cs, Predicate<Path> fileFilter, boolean printHashes
   ) throws IOException{
      Stream<Path> fs=Files.walk(root).filter(path->!Files.isDirectory(path));
      if(fileFilter!=null) fs=fs.filter(fileFilter);
      Iterator<Path> files=fs.iterator();
      while(files.hasNext()){
         Path path=files.next();
         File file=path.toFile();
         long flen=file.length();
         Results[] results=new Results[blockSizes.length];
         for(int i=0;i<blockSizes.length;i++){
            results[i]=new Results(
               path.toString(), new Date(),
               CompScan.getMapSupplier(flen/blockSizes[i])
            );
            results[i].set("block size", blockSizes[i]);
            results[i].set("superblock size", superblockSize);
         }
         for(AtomicLong hc:hashCounters) hc.set(0);
         try{
            scanStream(
               new MultipartByteStream.Adapter(
                  ByteStream.Util.convert(Files.newInputStream(path))
               ), results
            );
            for(Results r:results){
               r.set("files read", 1L);
               if(printHashes) r.printHashes();
            }
            cs.writeHashResults(results, path.relativize(root));
         }
         finally{
            for(Results r:results) r.releaseHashes();
         }
         fileResults.add(results);
      }
   }
}
