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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;

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
	
	private CompScan compScan;
	
	private Map<String, Boolean> assigned;
	
	private double ioRate;
	private Path pathIn;
	private Path pathOut;
   private ScanMode scanMode;
   private Object scanModeArg;
   private int blockSize;
	private int superblockSize;
	private String formatString;
	private int bufferSize;
	private boolean overwriteOK;
	private Compressor compressor;
	private boolean printHashes;
	private boolean verbose;
	private boolean printUsage;
	
	/**
	 * Constructor.
	 * 
	 * @param compScan CompScan instance to configure.
	 */
	public InputParser(CompScan compScan) {
		assigned = new HashMap<String, Boolean>();
		
		this.compScan = compScan;
		scanMode = ScanMode.NORMAL;
		ioRate = compScan.getIORate();
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
			if (arg.startsWith("-")) {
				parseOptional(arg, it);
			} else {
				parsePositional(arg, it, count++);
			}
		}
		
		checkPositionals();
		
      compScan.setup(
         ioRate, pathIn, pathOut, scanMode, scanModeArg, blockSize, superblockSize, 
         bufferSize, overwriteOK, compressor, printHashes, verbose, printUsage
		);
		printConfig();
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
				blockSize = Integer.parseInt(arg);
				if (blockSize < 1) {
					throw new NumberFormatException();
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
				if (superblockSize < 1 || superblockSize < blockSize) {
					throw new NumberFormatException();
				}
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
			compressor = new Compressor(blockSize, superblockSize, formatString);
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
				throw new IllegalArgumentException(
						String.format(
								"Optional parameter rate requires nonnegative integer (default: %1$f = unlimited MB/sec).",
								CompScan.UNLIMITED));
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
		// Default.
		default:
			throw new IllegalArgumentException(
					String.format(
							"Unknown optional argument \"%1$s\".", arg));
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
				"    - verbose:           %11$s%n",
				(ioRate == CompScan.UNLIMITED ? "UNLIMITED" : Double.toString(ioRate)),
				pathIn,
				pathOut,
				scanMode.toString(),
				blockSize,
				superblockSize,
				bufferSize,
				Boolean.toString(overwriteOK),
				Boolean.toString(printHashes),
				formatString,
				Boolean.toString(verbose)
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
