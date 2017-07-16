/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.lang.reflect.Field;
import net.deepstorage.compscan.util.*;

/**
 * CLI parser for CompScan.
 * 
 * @author Ramon A. Lovato
 */
public class InputParser {
	// Positional arguments.
	public static final String[] POSITIONAL_ARGS = {
		"pathIn",
		"pathOut",
		"blockSize",
		"superblockSize",
		"formatString"
	};
	
   private static long MAP_MEMORY_MAX=128*1024L*1024L*1024L*1024L;//128T: upper limit
   private static long MAP_MEMORY_CHUNK=128*1024L*1024L; //128M
   private static int MAP_LIST_SIZE=9;
   private static File MAP_DIR=new File(System.getProperty("user.dir"),".compscan/map");
   static{
      if(!MAP_DIR.exists()) MAP_DIR.mkdirs();
   }
   
   private static boolean pause=false;
 
	private CompScan compScan;
	
	private Map<String, Boolean> assigned;
	
	private Double ioRate;
	private Path pathIn;
	private Path pathOut;
   private ScanMode scanMode;
   private Object scanModeArg;
   private int[] blockSizes;
	private int superblockSize;
	private String formatString;
	private int bufferSize;
	private boolean overwriteOK;
	private Compressor compressor;
	private boolean printHashes;
	private boolean verbose;
	private boolean printUsage;
   private Path logPath;
   
	/**
	 * Constructor.
	 * 
	 * @param compScan CompScan instance to configure.
	 */
	public InputParser(CompScan compScan) {
		assigned = new HashMap<String, Boolean>();
		
		this.compScan = compScan;
		scanMode = ScanMode.NORMAL;
		bufferSize = CompScan.ONE_MB;
		overwriteOK = false;
		printHashes = false;
		verbose = false;
		printUsage = false;
		
		for (String s : POSITIONAL_ARGS) {
			if (!assigned.containsKey(s)) {
				assigned.put(s, false);
			}
		}
	}
	
	
	/**
	 * Parse CLI arguments. Deliberately package-private since it will be called by CompScan.
	 * 
	 * @param args Arguments to parse.
	 * @throws IllegalArgumentException if an argument is invalid or unrecognized.
	 */
	void parse(String[] args) throws IllegalArgumentException {
		if (args.length < POSITIONAL_ARGS.length) {
			throw new IllegalArgumentException(
					String.format(
							"Invalid number of arguments -- %1$d given, %2$d expected.",
							args.length, POSITIONAL_ARGS.length));
		}
		
		ListIterator<String> it = Arrays.asList(args).listIterator();
		int count = 0;
		while (it.hasNext()) {
			String arg = it.next();
			if(arg.startsWith("-")) {
				parseOptional(arg, it);
			}
			else{
				parsePositional(arg, it, count++);
			}
		}
		
      checkOptionals();
      checkPositionals();
      
      compScan.setup(
         ioRate, pathIn, pathOut, scanMode, scanModeArg,
         blockSizes, superblockSize, bufferSize, overwriteOK, 
         compressor, printHashes, verbose, printUsage, logPath
		);
		printConfig();
      
      //remove direct memory limit
      try{
         Class vm=Class.forName("sun.misc.VM");
         Field directMemory=vm.getDeclaredField("directMemory");
         directMemory.setAccessible(true);
         directMemory.set(null,MAP_MEMORY_MAX);
      }
      catch(Exception e){
         e.printStackTrace();
         System.out.println("Failed to change direct memory limit; use -XX:MaxDirectMemorySize=.. option to avoid OutOfMemoryException");
      }
      
      if(pause) try{
         System.out.println("press 'Enter' to continue");
         System.in.read();
      }
      catch(IOException e){}
   }
	
	/**
	 * Parse a single positional argument.
	 * 
	 * @param arg A single optional CLI argument.
	 * @param it A ListIterator into the argument list. Used for accessing the next argument
	 *           if necessary.
	 * @throws IllegalArgumentException if an argument isn't valid or recognized.
	 */
	private void parsePositional(String arg, ListIterator<String> it, int count)
			throws IllegalArgumentException {
		String key = (count < POSITIONAL_ARGS.length ? POSITIONAL_ARGS[count] : "");
		switch (key) {
		// Input path.
		case "pathIn":
			pathIn = Paths.get(arg);
			if (!Files.exists(pathIn)) {
				throw new IllegalArgumentException(
						String.format("Input path \"%1$s\" does not exist.",  arg));
			} else if (!isValidPath(pathIn)) {
				throw new IllegalArgumentException(
						String.format("Input path \"%1$s\" is not a valid directory, "
								+ ".vhd, .vhdx, or .vmdk.", arg));
			}
			break;
		// Output path.
		case "pathOut":
			pathOut = Paths.get(arg);
			if (!Files.isDirectory(pathOut)) {
				throw new IllegalArgumentException(
						String.format(
								"Output directory \"%1$s\" does not exist or is not a directory.",
								pathOut.toString()));
			}
			break;
		// Block size.
		case "blockSize":
			try {
			   String[] s=arg.split(",");
				blockSizes=new int[s.length];
            for(int i=0;i<s.length;i++){
               blockSizes[i]=Util.parseSize(s[i]).intValue();
               if(blockSizes[i] < 1) throw new NumberFormatException(s[i]);
            }
			} catch (NumberFormatException ex) {
				throw new IllegalArgumentException(
						String.format(
								"Block size must be a positive integer -- \"%1$s\" given.",
								arg));
			}
			break;
		// Superblock size.
		case "superblockSize":
			try {
				superblockSize = Integer.parseInt(arg);
            int maxBs=Arrays.stream(blockSizes).max().getAsInt();
            if (superblockSize < 1 || superblockSize < maxBs) throw new NumberFormatException(
               superblockSize+" ("+maxBs+")"
            );
			} catch (NumberFormatException ex) {
				throw new IllegalArgumentException(
						String.format(
								"Superblock size must be a positive integer >= block size -- "
								+ "\"%1$s\" given.",
								arg));
			}
			break;
		// Compression format.
		case "formatString":
			formatString = arg;
			compressor = new Compressor(superblockSize, formatString);
			break;
		// Default.
		default:
			throw new IllegalArgumentException(
					String.format(
							"Unknown positional argument: \"%1$s\".", arg));
					
		}
		// End switch.
		
		// Only reached if an exception wasn't thrown.
		assigned.replace(key, true);
	}
	
	/**
	 * Parse a single optional argument.
	 * 
	 * @param arg A single optional CLI argument.
	 * @param it A ListIterator into the argument list. Used for accessing the next argument
	 *           if necessary.
	 * @throws IllegalArgumentException if an argument isn't recognized.
	 */
	private void parseOptional(String arg, ListIterator<String> it) throws IllegalArgumentException {
		switch (arg) {
		// Help.
		case "-h": case "--help":
			CompScan.printHelp();
			System.exit(0);
		break;
		// Verbose mode.
		case "--verbose":
			verbose = true;
			break;
		// Memory usage estimate.
		case "--usage":
			printUsage = true;
			break;
		// VMDK mode.
		case "--mode":
         if(!it.hasNext()) throw new IllegalArgumentException(
            "--mode requires the following mode specifier: NORMAL, BIG, VMDK"
         );
         try{
            scanMode=ScanMode.valueOf(it.next());
         }
         catch(Exception e){
            throw new IllegalArgumentException("bad mode specifier, should be one of "+Arrays.asList(ScanMode.values()));
         }
         scanModeArg=scanMode.parseArg(it);
			break;
		// IO rate.
      case "--log":
         if(!it.hasNext()) throw new IllegalArgumentException(
            "missing file value to the log option"
         );
         logPath=Paths.get(it.next());
         break;
      case "--rate":
         if (!it.hasNext()) {
				throw new IllegalArgumentException(
						"Reached end of arguments without finding value for rate.");
			}
			try {
				ioRate = Double.parseDouble(it.next());
				if (ioRate < 0) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException ex) {
				throw new IllegalArgumentException(String.format(
               "Optional parameter rate requires nonnegative integer, MB/sec (default: unlimited)."
            ));
			}
			break;
		// Input buffer size.
		case "--buffer-size":
			if (!it.hasNext()) {
				throw new IllegalArgumentException(
						"Reached end of arguments without finding value for buffer size.");
			}
			try {
				bufferSize = Integer.parseInt(it.next());
				if (bufferSize < 1) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException ex) {
				throw new IllegalArgumentException(
						String.format(
								"Optional parameter buffer size requires positive integer (default: %d bytes).", CompScan.ONE_MB));
			}
			break;
		// Overwrite output.
		case "--overwrite":
			overwriteOK = true;
			break;
      case "--hashes":
         printHashes = true;
         break;
      case "--mapType":
         if(!it.hasNext()) throw new IllegalArgumentException(
            "Reached end of arguments for map type"
         );
         Supplier<MdMap> sup;
         String type=it.next();
         switch(type){
            case "java":
               sup=new JavaMapSupplier(SHA1Encoder.MD_SIZE);
               break;
            case "direct":
               sup=new DirectMapSupplier(
                  SHA1Encoder.MD_SIZE, MAP_LIST_SIZE,
                  Util.log(MAP_MEMORY_CHUNK), MAP_MEMORY_MAX
               );
               break;
            case "fs":
               sup=new FsMapSupplier(
                  SHA1Encoder.MD_SIZE, MAP_LIST_SIZE,
                  Util.log(MAP_MEMORY_CHUNK), MAP_MEMORY_MAX, MAP_DIR
               );
               break;
            default: throw new IllegalArgumentException("Illegal option for map type: "+type);
         }
         CompScan.bigMapSupplier=sup;
         break;
      case "--mapDir":
         if(!it.hasNext()) throw new IllegalArgumentException(
            "Reached end of arguments for map directory"
         );
         MAP_DIR=new File(it.next());
         break;
      case "--mapOptions":
         CompScan.printMapOptions();
         System.exit(0);
      case "--mapMemoryMax":
         if(!it.hasNext()) throw new IllegalArgumentException(
            "Reached end of arguments for map size"
         );
         MAP_MEMORY_MAX=Util.parseSize(it.next());
         break;
      case "--mapMemoryChunk":
         if(!it.hasNext()) throw new IllegalArgumentException(
            "Reached end of arguments for map chunk"
         );
         MAP_MEMORY_CHUNK=Util.parseSize(it.next());
         break;
      case "--mapListSize":
         if(!it.hasNext()) throw new IllegalArgumentException(
            "Reached end of arguments for map list size"
         );
         MAP_LIST_SIZE=Integer.parseInt(it.next());
         break;
      case "--threads":
         if(!it.hasNext()) throw new IllegalArgumentException(
            "Reached end of arguments for thread count"
         );
         Executor.setPoolSize(Integer.parseInt(it.next()));
         break;
      case "--pause":
         pause=true;
         break;
      // Default.
      default:
			throw new IllegalArgumentException(
					String.format(
							"Unknown optional argument \"%1$s\".", arg));
		}
	}
	
   private void checkOptionals(){
      if(CompScan.bigMapSupplier==null){
         CompScan.bigMapSupplier=new FsMapSupplier(
            SHA1Encoder.MD_SIZE, MAP_LIST_SIZE,
            Util.log(MAP_MEMORY_CHUNK), MAP_MEMORY_MAX, MAP_DIR
         );
      }
   }
   
   /**
	 * Check that all positional arguments were assigned.
	 */
	private void checkPositionals() {
		Queue<String> incompletes = new LinkedList<String>();
		for (String s : POSITIONAL_ARGS) {
			if (!assigned.containsKey(s) || assigned.get(s) == false) {
				incompletes.add(s);
			}
		}
		if (incompletes.size() > 0) {
			String incompleteString = String.format(
					"Missing the following positional arguments:%n    - " +
					String.join("%n    - ", incompletes) + "%n");
			throw new IllegalArgumentException(incompleteString);
		}
	}
	
	/**
	 * Print the current configuration.
	 */
	private void printConfig() {
		String setupString = String.format(
				"Configuration:%n" +
				"    - ioRate:            %1$s%n" +
				"    - pathIn:            %2$s%n" +
				"    - pathOut:           %3$s%n" +
				"    - scanMode:          %4$s%n" +
				"    - blockSize:         %5$s bytes%n" +
				"    - superblockSize:    %6$d bytes%n" +
				"    - bufferSize:        %7$d bytes%n" +
				"    - overwriteOK:       %8$s%n" +
				"    - printHashes:       %9$s%n" +
				"    - formatString:      %10$s%n" +
            "    - verbose:           %11$s%n"+
            "    - map type:          %12$s%n"+
            "    - threads:           %13$s%n",
            (ioRate == null ? "UNLIMITED" : Double.toString(ioRate)),
				pathIn,
				pathOut,
				scanMode.toString(),
				blockSizes,//todo
				superblockSize,
				bufferSize,
				Boolean.toString(overwriteOK),
				Boolean.toString(printHashes),
				formatString,
				Boolean.toString(verbose),
				CompScan.bigMapSupplier,
				Executor.getPoolSize()
				);
		System.out.println(setupString);
	}
	
	/**
	 * Check if path is valid.
	 * 
	 * @param path Path to verify.
	 * @return True if path is valid.
	 */
	private static Boolean isValidPath(Path path) {
		if (path == null) {
			return false;
		} else {
			return Files.exists(path);
		}
	}
}
