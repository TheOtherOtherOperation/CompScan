/**
 * 
 */
package net.deepstorage.compscan;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

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
	private Path root;
	private int blockSize;
	private int bufferSize;
	private Compressor compressor;
	private Results totals;
	private int superblockSize;
	private double ioRate;
	private boolean verbose;
	private MutableCounter hashCounter;
	
	/**
	 * Constructor.
	 * 
	 * @param root Root of the datastore to scan.
	 * @param bufferSize Size of the internal read buffer.
	 * @param ioRate Maximum IO rate in MB/s to throttle the scanning.
	 * @param compressor Compressor to use.
	 * @param totals Results object to update with the total scan data.
	 * @param hashCounter MutableCounter used for tracking the number of currently active unique hashes.
	 * @param verbose Whether or not to enable verbose logging.
	 */
	public FileScanner(Path root, int blockSize, int bufferSize, double ioRate, Compressor compressor,
			Results totals, MutableCounter hashCounter, boolean verbose) {
		this.root = root;
		this.blockSize = blockSize;
		this.bufferSize = bufferSize;
		this.verbose = verbose;
		
		this.compressor = compressor;
		this.totals = totals;
		this.hashCounter = hashCounter;
		superblockSize = compressor.getSuperblockSize();
		
		int remainder = bufferSize % superblockSize;
		if (remainder != 0) {
			this.bufferSize = bufferSize + superblockSize - remainder;
			System.out.format("Read buffer size adjusted to %s bytes to be an even multiple of superblock size.%n%n",
					          this.bufferSize);
		} else {
			this.bufferSize = bufferSize;
		}
		
		this.ioRate = ioRate;
	}
	
	/**
	 * Run scan.
	 * 
	 * @throws IOException if an IO error occurs.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 * @throws NoNextFileException if file root contains no regular files.
	 */
	public void scanCombined() throws IOException, BufferLengthException, NoNextFileException {
		try (FileWalkerStream fws = new FileWalkerStream(new FileWalker(root, verbose), blockSize, bufferSize, ioRate, false)) {
			if (!fws.hasMore()) {
				throw new NoNextFileException(
						String.format(
								"FileWalkerStream with root \"%s\" contains no scannable data.", root));
			}
			scanStream(fws, totals);
		}
		catch (IOException ex){
			throw ex;
		}
	}
	
	/**
	 * Scan a FileWalkerStream.
	 * 
	 * @param fws FileWalkerStream from which to pull scan data.
	 * @param r Results object to update with the scan data.
	 * @throws IOException if an IO error occurs.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 */
	private void scanStream(FileWalkerStream fws, Results r) throws IOException, BufferLengthException {
      JobGroup jg=new JobGroup();
		while(fws.hasMore()){
			byte[] buffer = fws.getBytes();
			Executor.exec(jg.addJob(new Runnable(){public void run(){
            scanBuffer(buffer, r);
            synchronized(FileScanner.this){
               r.set("files read", fws.getFilesRead());
               hashCounter.setCount(r.getHashes().size());
            }
         }}));
      }
      jg.waitAll();
      Executor.shutdown();
	}
	
	/**
	 * Run scan in VMDK mode.
	 * 
	 * To reduce the memory footprint, this mode saves results data to disk after each file.
	 * 
	 * @param fileResults List of Results in which to store the new scan results.
	 * @param cs CompScan responsible for saving results.
	 * @param printHashes Whether or not to print the hash table for each VMDK.
	 * @throws IOException if an IO error occurs.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 * @throws NoNextFileException if the file root contains no VMDKs.
	 */
	public void scanSeparately(
	   List<Results> fileResults, CompScan cs, Predicate<Path> fileFilter, boolean printHashes
	) throws IOException, BufferLengthException, NoNextFileException {
		// scanFile will use the local verbose field, so to prevent double printing, always use false for
		// this walker.
      try (FileWalker fw = new FileWalker(root, fileFilter, false)) {
			if (!fw.hasNext()){
				throw new NoNextFileException(
						String.format(
								"FileWalker opened in separate file mode with root \"%s\" but contains no files of specified type.", root));
			}
			while(fw.hasNext()){
				Path f = fw.next();
				Results r = new Results(f.toString(), totals.getTimestamp());
				r.set("block size", totals.get("block size"));
				r.set("superblock size", totals.get("superblock size"));
				hashCounter.resetCount();
				scanFile(f, r);
			   r.set("files read", 1L);
            cs.writeHashResults(r, f);
            if(printHashes){
               r.printHashes();
            }
            r.releaseHashes();
            fileResults.add(r);
			}
		}
		catch(IOException ex){
			throw ex;
		}
	}
	
	/**
	 * Scan a single file.
	 * 
	 * @param f Path to the file to scan.
	 * @param r Results object to update with the scan data.
	 * @param verbose Whether or not to enable verbose logging.
	 * @throws IOException if an IO error occurs.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 */
	private void scanFile(Path f, Results r) throws IOException, BufferLengthException {
		if(f==null || r==null){
			return;
		}
      totals.incrementFilesRead();
		try(FileWalkerStream fws = new FileWalkerStream(new FileWalker(f, verbose), blockSize, bufferSize, ioRate, true)) {
         JobGroup jg=new JobGroup();
         while (fws.hasMore()){
            final byte[] buffer = fws.getBytes();
            final Results intermediate = new Results(f.toString(), r.getTimestamp());
				Executor.exec(jg.addJob(new Runnable(){public void run(){
               scanBuffer(buffer, intermediate);
               synchronized(FileScanner.this){
                  r.feedOtherResults(intermediate, intermediate.getHashes());
                  totals.feedOtherResults(intermediate, null);
                  hashCounter.setCount(r.getHashes().size());;
               }
            }
            }));
			}
         jg.waitAll();
      }
	}
	
	/**
	 * Scan a data buffer by splitting it into superblocks. The buffer size is automatically rounded up
	 * to the next even multiple of the superblock size, making this easy.
	 * 
	 * @param b Data buffer to scan. Must have length == bufferSize.
	 * @param r Results object to update with scan results.
	 * @throws BufferLengthException if the buffers are the wrong size.
	 */
	private void scanBuffer(byte[] b, Results r) throws BufferLengthException {
		if (b.length != bufferSize) {
			throw new BufferLengthException(
					String.format(
							"Input buffer size is %1$d but data buffer provided is size %2$d.",
							bufferSize, b.length));
		}
		for (int i = 0, j = superblockSize; j <= b.length; i += superblockSize, j += superblockSize) {
			byte[] segment = Arrays.copyOfRange(b, i, j);
			scanSuperblock(segment, r);
		}
	}
	
	/**
	 * Scan a single superblock of data.
	 * 
	 * @param segment Data buffer to scan. Must be one superblock in length.
	 * @param r Results object to update with scan results.
	 * @throws BufferLengthException if the buffers are the wrong size.
	 */
	private void scanSuperblock(byte[] segment, Results r) throws BufferLengthException {
		if (segment.length != superblockSize) {
			throw new BufferLengthException(
					String.format(
							"Superblock size is %1$d but data buffer provided is size %2$d.",
							superblockSize, segment.length));
		}
		CompressionInfo ci = compressor.feedData(segment);
		r.feedCompressionInfo(ci);
	}
	
	/**
	 * A custom exception for handling no next file.
	 */
	public static class NoNextFileException extends Exception {
		public NoNextFileException(String message) {
			super(message);
		}
	}
	
}
