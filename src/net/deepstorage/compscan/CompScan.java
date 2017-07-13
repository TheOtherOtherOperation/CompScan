/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.function.Predicate;
import java.util.function.Supplier;

import net.deepstorage.compscan.Compressor.BufferLengthException;
import net.deepstorage.compscan.Compressor.CompressionInfo;
import net.deepstorage.compscan.util.*;

/**
 * CompScan's main class.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class CompScan {
	// Subpackage prefix for the compression package.
	public static final String COMPRESSION_SUBPACKAGE = "compress";
	// Symbolic constant for 1 million bytes (1 MB), the default input buffer size.
	public static final int ONE_MB = 1_000_000;
	
   //Map suppliers for whole stream (big), single file (medium), single superblock (small)
	//inject!!!
   public static Supplier<MdMap> bigMapSupplier;
   
   public static final Supplier<MdMap> mediumMapSupplier=
      new DirectMapSupplier(20,8,26,1024*1024*1024) //key size 20B, value size 8B, grow in 64M chunks, limit 1G
   ;
   public static final Supplier<MdMap> smallMapSupplier=
      new JavaMapSupplier(20) //use java HashMap
   ;
   
   //over this threshold use bigMapSupplier
   private static final int bigMapSize= 256*1024*1024/120; // 256M/(max entry size)
   //below this threshold use smallMapSupplier
   private static final int smallMapSize=2000; //limit by load to gc
	
   public static Supplier<MdMap> getMapSupplier(long size){
      if(size>=bigMapSize) return bigMapSupplier;
      if(size<=smallMapSize) return smallMapSupplier;
      return mediumMapSupplier;
   }
	
	private final Date date;
	
	private boolean setupLock;
	
	private Double ioRate;
	private Path pathIn;
	private Path pathOut;
   private ScanMode scanMode;
   Object scanModeArg;
   private int[] blockSizes;
   private int superblockSize;
   private int bufferSize;
	private boolean overwriteOK;
	private Compressor compressor;
	private boolean printHashes;
   private boolean verbose;
   private AtomicLong[] hashCounters;
	private boolean printUsage;
   private Path logPath;
   
	/**
	 * Default constructor.
	 * 
	 * @param args CLI arguments.
	 */
	private CompScan(String[] args) throws IllegalArgumentException {
		scanMode = ScanMode.NORMAL;
      bufferSize = ONE_MB;
		
		date = Calendar.getInstance().getTime();
		InputParser ip = new InputParser(this);
		ip.parse(args);
	}

	/**
	 * Allows other package members, notably the InputParser, to set fields. Deliberately package-private.
	 * 
	 * @param ioRate IOPS limit.
	 * @param pathIn Input path.
	 * @param pathOut Output path.
	 * @param scanMode ScanMode to use.
	 * @param blockSize Block size in bytes.
	 * @param superblockSize Superblock size in bytes.
	 * @param bufferSize Size of the internal read buffer.
	 * @param overwriteOK Whether it's allowed to overwrite the output file.
	 * @param compressor Compressor associated with formatString.
	 * @param printHashes Whether or not to print the hash table.
	 * @param verbose Whether or not to enable verbose console logging.
	 * @param printUsage Whether or not to include estimated memory usage in console output.
	 * @throws Exception if called more than once.
	 */
	void setup(
      Double ioRate, Path pathIn, Path pathOut, ScanMode scanMode, Object scanModeArg,
      int[] blockSizes, int superblockSize, int bufferSize, boolean overwriteOK, 
      Compressor compressor, boolean printHashes, boolean verbose, boolean printUsage,
      Path logPath
	){
		if (setupLock) {
			System.err.println("CompScan.setup cannot be called more than once.");
			System.exit(1);
		}
		this.ioRate = ioRate;
		this.pathIn = pathIn;
		this.pathOut = pathOut;
		this.scanMode = scanMode;
      this.scanModeArg=scanModeArg;
      this.blockSizes= blockSizes;
      this.superblockSize=superblockSize;
		this.bufferSize = bufferSize;
		this.overwriteOK = overwriteOK;
		this.compressor = compressor;
		this.printHashes = printHashes;
		this.verbose = verbose;
		this.printUsage = printUsage;
      this.logPath=logPath;
		setupLock = true;
		
      if(bigMapSupplier==null) throw new IllegalStateException("missing map supplier");
   }
	
	/**
	 * Run scan.
	 */
	void run() {
		System.out.format("Starting run.%n%n");

      Results results[] = new Results[blockSizes.length];
      for(int i=0;i<results.length;i++){
         Results r=results[i]=
            new Results(pathOut.getFileName().toString(), date, bigMapSupplier)
         ;
         r.set("block size", blockSizes[i]);
         r.set("superblock size", superblockSize);
      }
      hashCounters=new AtomicLong[blockSizes.length];
      for(int i=0;i<hashCounters.length;i++) hashCounters[i]=new AtomicLong();
      ConsoleDisplayThread cdt = new ConsoleDisplayThread(results, hashCounters, printUsage, logPath);
      try{
         try{
   			FileScanner fs = new FileScanner(
               pathIn, blockSizes, superblockSize, bufferSize, results, hashCounters, compressor, 
               ioRate, verbose
   		   );
            cdt.start();
            fs.scanCombined();
            if(printHashes) printHashes(results);
   		}
   		catch (IOException e) {
   			System.err.format("A filesystem IO error ocurred.%n%n");
   			e.printStackTrace();
   			System.exit(1);
   		}
   		catch (BufferLengthException e) {
   			System.err.format("The input buffer is the wrong size.%n%n");
   			e.printStackTrace();
   			System.exit(1);
   		}
   		cdt.interrupt();
   		try{
   			cdt.join();
   		}
   		catch (InterruptedException e) {}
      		
   		try{
   		   //hashes
            for(int i=0;i<blockSizes.length;i++){
               writeResults("hashes_"+blockSizes[i]+".csv", results[i].makeHashCounterString(), overwriteOK);
               writeResults("totals_"+blockSizes[i]+".csv", results[i].toString(), overwriteOK);
            }
            //compression
            System.out.println(String.format("%n--> Output saved in \"%s\".%n", pathOut));
         }
   		catch (IOException e) {
   			System.err.println("Unable to save output.");
   			e.printStackTrace();
   		}
   		finally{
            for(Results r:results) System.out.println(r.toString());
   		}
	   }
      finally{
         for(Results r:results) r.releaseHashes();
         Executor.shutdown();
      }
   }
	
	/**
	 * Run VMDK-mode scan.
	 */
	void runFiles(Predicate<Path> fileFilter) {
		System.out.format("Starting run.%n%n");
		
      Results[] totals = new Results[blockSizes.length];
      for(int i=0;i<totals.length;i++){
         totals[i]=new Results(
            pathOut.getFileName().toString(), date, bigMapSupplier
         );
         totals[i].set("block size", blockSizes[i]);
         totals[i].set("superblock size", superblockSize);
      }
      hashCounters=new AtomicLong[blockSizes.length];
      for(int i=0;i<hashCounters.length;i++) hashCounters[i]=new AtomicLong();
      List<Results[]> allResults = new LinkedList<>();
      ConsoleDisplayThread cdt = new ConsoleDisplayThread(totals, hashCounters, printUsage, logPath);
      try{
   		try{
   			FileScanner fs = new FileScanner(
               pathIn, blockSizes, superblockSize, bufferSize, totals, hashCounters,
               compressor, ioRate, verbose
   			);
            cdt.start();
            fs.scanSeparately(allResults, this, fileFilter, printHashes);
   		}
   		catch (IOException e) {
   			System.err.format("A filesystem IO error ocurred.%n%n");
   			e.printStackTrace();
   			System.exit(1);
   		}
   		catch (BufferLengthException e) {
   			System.err.format("The input buffer is the wrong size.%n%n");
   			e.printStackTrace();
   			System.exit(1);
   		}
   		cdt.interrupt();
   		try{
   			cdt.join();
   		} catch (InterruptedException e) {
   			// Nothing to do.
   		}
   		
   		try {
            // Hash results are saved incrementally in VMDK mode, so don't need to do anything here.
            // now save compression results
            for(int i=0;i<blockSizes.length;i++){
               final int ind=i;
               String resultString=makeFileResultString(
                  allResults.stream().map(results->results[ind]).collect(Collectors.toList()),
                  totals[i]
               );
               writeResults("totals_"+blockSizes[i]+".csv", resultString, overwriteOK);
               System.out.println(resultString);
            }
            System.out.println(String.format("%n--> Output saved in \"%s\".%n", pathOut));
   		}
   		catch (IOException e) {
   			System.err.println("Unable to save output.");
   			e.printStackTrace();
   		}
      }
      finally{
         for(Results r:totals) r.releaseHashes();
         Executor.shutdown();
      }
	}
   
	/**
	 * Convert VMDK scan results into a string for saving to the output CSV.
	 * 
	 * @param allResults List of Results objects containing the intermediate results.
	 * @param totals Results object containing the aggregate results.
	 */
	public String makeFileResultString(List<Results> allResults, Results totals) {
		List<String> lines = new LinkedList<>();
		lines.add(totals.makeHeadingString());
		lines.addAll(
				allResults.stream()
				.map(r -> r.makeValueString())
				.collect(Collectors.toList()));
		lines.add("");
		lines.add("--- Totals ---");
		lines.add(totals.makeValueString());
		
		return String.join(System.lineSeparator(), lines);
	}
   
   public void printHashes(Results[] results)throws IOException{
      for(int i=0;i<results.length;i++){
         System.out.println("Hashes for block size "+blockSizes[i]+":");
         results[i].printHashes();
      }
   }
   
	/**
	 * Save the results to a CSV file.
	 * 
	 * @param name Filename.
	 * @param resultString Results string to save.
	 * @return The actual path where the file was saved. Might not be the same as pathOut if pathOut already exists.
	 * @throws IOException if an IO error occurred.
	 */
   public String writeResults(String name, String resultString) throws IOException {
      return writeResults(name, resultString, overwriteOK);
   }
   public String writeResults(String name, String resultString, boolean overwriteOK) throws IOException {
      Path writePath = pathOut.resolve(name);
		if (Files.exists(writePath) && !overwriteOK) {
			int i = 0;
			String[] partials = name.split("\\.(?=\\w+$)");
			while (Files.exists(writePath)) {
				i++;
				String pathString = (partials.length < 2 ?
						String.format("%1$s (%2$d).%3$s", partials[0], i, partials[1]) :
							String.format("%1$s %2$d", partials[0], i));
				writePath = pathOut.resolve(pathString);
			}
		}
		
		try (BufferedWriter bw = Files.newBufferedWriter(writePath)) {
			bw.write(resultString);
		} catch (IOException e) {
			throw e;
		}
		return writePath.toString();
	}
	
	/**
	 * Write the hash results to a file.
	 * 
	 * @param r Results object containing the hash counter map.
	 * @param p Where to save the output file.
	 * @return Where the file was saved.
	 * @throws IOException
	 */
	public String writeHashResults(Results r, Path p) throws IOException {
		return writeResults(p.getFileName() + ".hash.csv", r.makeHashCounterString(), overwriteOK);
	}
   
   public void writeHashResults(Results[] results, Path path) throws IOException{
      final String fs=path.getFileSystem().getSeparator();
      for(int i=0;i<results.length;){
         Results r=results[i];
         String fileName=
            path.toString().replace(fs,"_")+"_"+r.get("block size")+".hash.csv"
         ;
         writeResults(fileName, r.makeHashCounterString(), overwriteOK);
      }
   }
   
	/**
	 * Getter for ioRate.
	 * 
	 * @return The IOPS limit (0 = UNLIMITED).
	 */
	public Double getIORate() {
		return ioRate;
	}
	
	/**
	 * Print help message.
	 * 
	 * @param custom a custom message to print
	 */
	public static void printHelp(String custom) {
		System.out.format(
            "Usage: CompScan [-h] [--help] [--mode (NORMAL|BIG <size>|VMDK)]"
            + " [--overwrite] [--rate MB_PER_SEC] [--buffer-size BUFFER_SIZE]"
            + " [--mapType (java|direct|fs[:<path>])] [--mapDataSizeMax <size>]"
            + " [--mapDataChunk <size>] [--mapListSize <number>] [--threads <number>]"
            + " pathIn pathOut blockSize superblockSize format[:<options>]%n"
				+ "Positional Arguments%n"
			   + "         pathIn            path to the dataset%n"
				+ "         pathOut           where to save the output%n"
            + "         blockSize         bytes per block; scanning with multiple block sizes%n"
            + "                           is supported. Format: one or more integer values%n"
            + "                           separated by commas without spaces;%n"
            + "                           each value may have scale suffix (k for KiB, etc)%n"
            + "                           example: \"512,1k,2k\"%n"
            + "         superblockSize    bytes per superblock%n"
            + "         format            compression format to use, currently%n"
            + "                           includes None,LZ4,GZIP,LZW;%n"
            + "         format options    for LZ4: compression level, number 0 - 17, default 9%n"
            + "                           for GZIP: compression level, number 1 - 9, default 6%n"
      + "Optional Arguments%n"
				+ "         -h, --help        print this help message%n"
			   + "         --verbose         enable verbose console feedback (should only be used for debugging)%n"
 				+ "         --usage           enable printing of estimated memory usage (requires wide console)%n"
            + "         --mode            scan mode, one of "+Arrays.asList(ScanMode.values())+":%n"
            + "                           * NORMAL - default, all file tree is processed as single stream%n"
            + "                           * BIG <size spec> - process files independently, only those%n"
            + "                             bigger then spec, where <size spec> = <number>[k|m|g]%n"
            + "                             meaning size in bytes, e.g: 1000, 100k, 100m, 100g%n"
            + "                           * VMDK - process files independently, only those%n"
            + "                             with extensions "+ScanMode.VMDK_EXTENSIONS+"%n"
            + "         --overwrite       whether overwriting the output file is allowed%n"
            + "         --report <file>   print progress info to the file%n"
            + "         --rate MB_PER_SEC maximum MB/sec we're allowed to read%n"
			   + "         --buffer-size BUFFER_SIZE size of the internal read buffer%n"
            + "         --hashes          print the hash table before exiting; the hashes are never saved to disk%n"
            + "         --threads         number of processing threads.%n"
            + "                           Optimal value: number of CPU cores/hyperthreads.%n"
            + "                           Default: autodetected%n"
            + "         --pause           make pause (e.g. to attach external profiler)%n"
            + "         --mapType         select the implementation of the hash-counting map:%n"
            + "                           * java - plain java.util.HashMap%n"
            + "                           * direct - custom off-heap map over native RAM%n"
            + "                           * fs - custom off-heap map over memory-mapped files%n"
            + "                           Default: fs:<user home>/.compscan/map%n"
            + "         --mapDir          home directory for fs-type map%n"
            + "         --mapOptions      print advanced map options and exit%n"
      );
		// Short-circuits.
		if (custom != null && custom.length() > 0) {
			System.out.format("%n" + custom + "%n");
		}
	}
	
	static void printMapOptions(){
      System.out.format(
            "Advanced map options:"
            + "         --mapMemoryMax    upper expected limit for map memory size.%n"
            + "                           Holds only for direct and fs maps. Upon reaching%n"
            + "                           this limit the maps will stop working.%n"
            + "                           Estimation: <expected scan size>*100/<block size>%n"
            + "                           Default: 128TiB%n"
            + "         --mapMemoryChunk  map memory allocation chunks.%n"
            + "                           Holds only for direct and fs maps.%n"
            + "                           Too small chunks may cause speed degradation.%n"
            + "                           Too large chunks may cause allocation problems.%n"
            + "                           Optimal value:%n"
            + "                           <expected map memory size (see above)>/1000%n"
            + "                           Default: 128MiB%n"
            + "         --mapListSize     map's internal parameter, controls the tradeoff%n"
            + "                           between speed and memory consumtion%n"
            + "                           (both are inversely proportional to it)%n"
            + "                           Optimal range: 8..20%n"
            + "                           Default: 9%n"
      );
	}
   
	/**
	 * Convenience overload for printHelp.
	 */
	public static void printHelp() {
		printHelp(null);
	}

	/**
	 * Main method.
	 * 
	 * @param args CLI arguments.
	 */
	public static void main(String[] args) throws Exception{
//if(args.length==0){
//   String testdata= new File("testdata").exists()? "testdata": "../testdata";
//   //args=new String[]{testdata+"/in/all",testdata+"/out","1000","10000","None"};
//   args=new String[]{
//      "--hashes","--overwrite",
//      testdata+"/in/small",testdata+"/out","1000,1500,2000","10000","LZ4:0"
//   };
//}
      CompScan cs = null;
		try{
			cs = new CompScan(args);
		}
		catch (IllegalArgumentException ex) {
			printHelp(ex.getMessage());
		}
		if (cs == null) {
			System.exit(1);
		}
      
      cs.scanMode.run(cs);
	}
	
 /**
	 * A simple inner class for keeping track of the current hash count.
	 */
	public class MutableCounter{
		private long c;
		
		public MutableCounter() {
			resetCount();
		}
		
		public void setCount(long c) {
			this.c = c;
		}
		
		public long getCount() {
			return c;
		}
		
		public void resetCount() {
			c = 0L;
		}
		
		public void addCount(long c) {
			this.c += c;
		}
	}
	
	/**
	 * A simple static nested class for encapsulating test results.
	 */
	public static class Results {
		private final String[] KEYS = {
				"files read",
				"block size",
				"superblock size",
				"bytes read",
				"blocks read",
				"superblocks read",
				"compressed bytes",
				"compressed blocks",
				"actual bytes needed"
		};
		private final String name;
		private final String timestamp;
		private Map<String, Long> map;
      
      //private Map<Object, Long> hashes;
      private MdMap hashes;
		
		/**
		 * Convenience constructor for creating a new Results object from a name
		 * and Date object.
		 * 
		 * @param name Name to assign to the resulting data set.
		 * @param date Date object from which to generate the timestamp.
		 */
		public Results(String name, Date date, Supplier<MdMap> regSup) {
         this(name, new SimpleDateFormat("MM/dd/yyyy KK:mm:ss a Z").format(date),regSup);
		}
		
		/**
		 * Create a new Results object.
		 * 
		 * @param name Name to assign to the resulting data set.
		 * @param timestamp Formatted timestamp string to use.
		 */
      public Results(String name, String timestamp, Supplier<MdMap> regSup) {
			this.name = name;
			this.timestamp = timestamp;
			// Uses a LinkedHashMap to preserve insertion order.
			map = new LinkedHashMap<>();
			for (String s : KEYS) {
				map.put(s, 0L);
			}
         hashes = regSup.get();
		}
		
		/**
		 * Set the value for a given map key.
		 * 
		 * @param k Map key to update.
		 * @param v Value to assign to the key.
		 */
      public synchronized void set(String k, long v) {
//public void set(String k, long v) {
         if (!map.containsKey(k)) {
				throw new IllegalArgumentException("Invalid key \"" + k + "\".");
			}
			map.put(k, v);
		}
		
		/**
		 * Get the value for a given map key.
		 * 
		 * @param k Map key to retrieve.
		 * @return Value assigned to the key.
		 */
      public synchronized long get(String k) {
//public long get(String k) {
         if (!map.containsKey(k)) {
				throw new IllegalArgumentException("Invalid key \"" + k + "\".");
			}
			return map.get(k);
		}
		
		/**
		 * Get the timestamp.
		 * 
		 * @return A formatted timestamp string.
		 */
		public String getDate() {
			return timestamp;
		}
		
		/**
		 * Increase the hash counter for the specified hash. If the hash is not
		 * already in the hashes map, it gets added first.
		 * 
		 * @param hash Sha-1 hash to update.
		 * @param count Number to add to the hash counter.
		 */
      public synchronized void updateHash(byte[] hash, long count) {
//public void updateHash(Object hash, long count) {
         hashes.add(hash,0,count);
      }
      
      public synchronized void updateHashes(List<byte[]> list){
//public void updateHash(Object hash, long count) {
         for(int i=list.size(); i-->0;) hashes.add(list.get(i),0,1);
      }
      
  /**
		 * Run updateHash on all entries in the specified map.
		 * 
		 * @param h Map<String, Long> containing counters for hash occurrences.
		 */
      public synchronized void updateHashes(MdMap h){
//public void updateHashes(Map<Object, Long> h) {
         if(h == null) return;
			h.scan(new MdMap.Consumer(){
            public void consume(byte[] md, int off, long count){
               hashes.add(md,off,count);
            }
         });
		}
      
      public synchronized void updateHashes(Map<MD,Long> h){
         if(h==null) return;
         for(Map.Entry<MD,Long> e: h.entrySet()){
            MD md=e.getKey();
            hashes.add(md.data, md.off, e.getValue());
         }
      }
		/**
		 * Allows releasing resources for the hash counters.
		 */
      public void releaseHashes(){
         hashes.dispose();
		}
		
		/**
		 * Feed a CompressionInfo object into the Results to update the counters.
		 * 
		 * @param ci CompressionInfo whose data should be added to the results.
		 */
      public synchronized void feedCompressionInfo(CompressionInfo ci) {
//public void feedCompressionInfo(CompressionInfo ci) {
         addTo("bytes read", ci.bytesRead);
			addTo("blocks read", ci.blocksRead);
			addTo("superblocks read", ci.superblocksRead);
			addTo("compressed bytes", ci.compressedBytes);
			addTo("compressed blocks", ci.compressedBlocks);
			addTo("actual bytes needed", ci.actualBytes);
		}
		
		/**
		 * Feed another Results object into the Results to update the counters.
		 * 
		 * @param r Results object from which to update.
		 */
      public synchronized void feedOtherResults(Results r, boolean hashes){
//public void feedOtherResults(Results r) {
         feedOtherResults(r, hashes? r.hashes: null);
		}
		
		/**
		 * Feed another Results object into the Results to update the counters.
		 * 
		 * @param r Results object from which to update.
		 * @param h Hashes map from which to update hashes.
		 */
      private void feedOtherResults(Results r, MdMap h){
//public void feedOtherResults(Results r, Map<Object, Long> h) {
         addTo("files read", r.get("files read"));
			addTo("bytes read", r.get("bytes read"));
			addTo("blocks read", r.get("blocks read"));
			addTo("superblocks read", r.get("superblocks read"));
			addTo("compressed bytes", r.get("compressed bytes"));
			addTo("compressed blocks", r.get("compressed blocks"));
			addTo("actual bytes needed", r.get("actual bytes needed"));
			if (h != null) {
				updateHashes(h);
			}
		}
		
		/**
		 * Increment the files read counter.
		 */
      public synchronized void incrementFilesRead() {
//public void incrementFilesRead() {
         set("files read", get("files read") + 1);
		}
		
		/**
		 * Get the raw compression factor.
		 * 
		 * @return Compressed bytes / bytes read.
		 */
      public synchronized double getRawCompressionFactor() {
//public synchronized double getRawCompressionFactor() {
         long bytesRead = map.get("bytes read");
			if (bytesRead == 0) {
				return 0.0;
			}
			return ((double) map.get("compressed bytes")) / ((double) bytesRead);
		}
		
		/**
		 * Get the superblock compression factor.
		 * 
		 * @return Actual bytes needed / bytes read.
		 */
      public synchronized double getSuperblockCompressionFactor() {
//public double getSuperblockCompressionFactor() {
         long bytesRead = map.get("bytes read");
			if (bytesRead == 0) {
				return 0.0;
			}
			return ((double) map.get("actual bytes needed")) / ((double) bytesRead);
		}
		
		/**
		 * Generate a heading string.
		 * 
		 * @return A CSV-formatted heading string from the map keys.
		 */
      public synchronized String makeHeadingString() {
//public String makeHeadingString() {
         List<String> headings = new LinkedList<>(map.keySet());
			headings.add(0, "name");
			headings.add(1, "timestamp");
			headings.add("raw compression factor");
			headings.add("superblock compression factor");
			return String.join(",",  headings);
		}
		
		/**
		 * Generate a value string.
		 * 
		 * @return A CSV-formatted value string from the map values.
		 */
      public synchronized String makeValueString() {
//public String makeValueString() {
         List<String> values = new LinkedList<>(
					map.values()
					.stream()
					.map(v -> String.valueOf(v))
					.collect(Collectors.toList()));
			values.add(0, String.format("\"%s\"", name));
			values.add(1, timestamp);
			values.add(String.valueOf(getRawCompressionFactor()));
			values.add(String.valueOf(getSuperblockCompressionFactor()));
			return String.join(",",  values);
		}
		
		/**
		 * Generate a string for the hash counters.
		 * 
		 * @return A CSV-formatted string for the hash counters.
		 */
      public synchronized String makeHashCounterString() {        
         Map<Long, Long> counters = getHashCounters();
			
			List<String> lines = new LinkedList<String>();
			
			lines.add("number of blocks,number of repeats");
			lines.addAll(counters.entrySet()
					.stream()
					// Since the counters are returned as a mapping of no. of repeats --> no. of blocks, we need
					// to print the values before the keys.
					.map(e -> String.format("%d,%d", e.getValue(), e.getKey()))
					.collect(Collectors.toList()));
			
			return String.join(System.lineSeparator(), lines);
		}
		
		/**
		 * Get the hash counts.
		 * 
		 * @return Map<Long, Long> in which the key represents the number of repeats and the value
		 * 		   represents the number of blocks that repeat that number of times.
		 */
      public synchronized Map<Long, Long> getHashCounters() {
//public Map<Long, Long> getHashCounters() {
         Map<Long, Long> counters = new TreeMap<Long, Long>();
         hashes.scan(new MdMap.Consumer(){
			   public void consume(byte[] md, int off, long count){
               Long n=counters.get(count);
               counters.put(count, n==null? 1L: n+1L);
            }
			});
System.out.println("CS.Results.getHashCounters():");
System.out.println(" hashes: "+hashes.size());
System.out.println(" counters: "+counters);
         return counters;
		}
		
		@Override
      public synchronized String toString() {         
			return makeHeadingString() + System.lineSeparator() + makeValueString();
		}
		
		/**
		 * Add a value to one one of the map values.
		 * 
		 * @param k Map key to add to.
		 * @param v Value to add to the map value for k.
		 */
      private void addTo(String k, long v) {
			map.put(k, map.get(k) + v);
		}

		/**
		 * Getter for the timestamp string.
		 * 
		 * @return The timestamp string.
		 */
		public String getTimestamp() {
			return timestamp;
		}
		
		/**
		 * Getter for the hash counters map.
		 * 
		 * @return The hash counters map.
		 */
      public MdMap getHashes() {
			return hashes;
		}
		
		/**
		 * Build a formatted string of the hash counters map.
		 * 
		 * @return Formatted string of the hash counters map.
		 */
		public synchronized String makeHashString(){
         StringBuilder sb=new StringBuilder();
		   hashes.scan(new MdMap.Consumer(){
            public void consume(byte[] md, int off, long count){
               sb.append(Util.toHexString(md,off,hashes.keyLength()));
               sb.append(" -> ");
               sb.append(count);
               sb.append("\n");
            }
		   });
		   return sb.toString();
		}
		
		/**
		 * Debugging method for printing the hash counters map.
		 */
		public void printHashes() {
			System.out.println(makeHashString());
		}
	}
}
