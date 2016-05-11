/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CompScan's main class.
 * 
 * @author Ramon A. Lovato (ramonalovato.com)
 * @version 1.0
 */
public class CompScan {
	// Expected number of arguments.
	public static final int MIN_ARGS = 5;
	// Positional arguments.
	public static final String[] POSITIONAL_ARGS = {
		"pathIn",
		"pathOut",
		"blockSize",
		"superblockSize",
		"format"
	};
	// Valid file extensions.
	public static final String[] VALID_EXTENSIONS = {
			".vhd",
			".vhdx",
			".vmdk"
	};
	// Default buffer size: 1MB.
	public static final int DEFAULT_BUFFSIZE = 1000000;
	// Symbolic constant representing unthrottled IO rate.
	public static final int UNLIMITED = 0;
	
	private int buffSize;
	private int ioRate;
	
	private Date date;
	private Path pathIn;
	private Path pathOut;
	private int blockSize;
	private int superblockSize;
	private String format;
	private boolean vmdkMode;
	
	/**
	 * Default constructor.
	 * 
	 * @param args CLI arguments.
	 */
	private CompScan(String[] args) throws IllegalArgumentException {
		date = Calendar.getInstance().getTime();
		vmdkMode = false;
		buffSize = DEFAULT_BUFFSIZE;
		ioRate = UNLIMITED;
		parseArgs(args);
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
		case "-h": case "--help":
			printHelp(null);
			System.exit(0);
		break;
		// VMDK.
		case "--vmdk":
			vmdkMode = true;
			break;
		// IO rate.
		case "--rate":
			if (!it.hasNext()) {
				throw new IllegalArgumentException(
						"Reached end of arguments without finding value for rate.");
			}
			try {
				ioRate = Integer.parseInt(it.next());
				if (ioRate < 0) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException ex) {
				throw new IllegalArgumentException(
						String.format(
								"Optional parameter rate requires nonnegative integer (default: %1$d = unlimited MB/sec).",
								UNLIMITED));
			}
			break;
		// Buffer size.
		case "--buffer-size":
			if (!it.hasNext()) {
				throw new IllegalArgumentException(
						"Reached end of arguments without finding value for buffer size.");
			}
			try {
				buffSize = Integer.parseInt(it.next());
				if (buffSize < 1) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException ex) {
				throw new IllegalArgumentException(
						String.format(
								"Optional parameter buffer size requires nonnegative integer (default: %1$d bytes).",
								DEFAULT_BUFFSIZE));
			}
			break;
		// Default.
		default:
			throw new IllegalArgumentException(
					String.format(
							"Unknown optional argument \"%1$s\".", arg));
		}
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
			if (Files.exists(pathOut)) {
				throw new IllegalArgumentException(
						String.format("Output path \"%1$s\" already exist.", arg));
			} else if (!Files.isDirectory(pathOut.getParent())) {
				throw new IllegalArgumentException(
						String.format(
								"Output directory \"%1$s\" does not exist.",
								pathOut.getParent().toString()));
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
		case "format":
			format = arg;
			break;
		// Default.
		default:
			throw new IllegalArgumentException(
					String.format(
							"Unknown positional argument: \"%1$s\".", arg));
					
		}
		// End switch.
	}
	
	/**
	 * Parse CLI arguments.
	 * 
	 * @param args CLI arguments.
	 * @throws IllegalArgumentException if an argument is invalid or unrecognized.
	 */
	private void parseArgs(String[] args) throws IllegalArgumentException {
		if (args.length < MIN_ARGS) {
			throw new IllegalArgumentException(
					String.format(
							"Invalid number of arguments -- %1$d given, %2$d expected.",
							args.length, MIN_ARGS));
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
		
		if (vmdkMode && !isVMDK(pathIn)) {
			throw new IllegalArgumentException(
					String.format(
							"VMDK mode specified but \"%1$d\" does not appear to be a "
							+ "valid .vhd, .vhdx, or .vmdk.",
							pathIn));
		}
	}
	
	/**
	 * Run scan.
	 * 
	 * @return Results object containing the test results.
	 */
	private Results run() {
		Results results = new Results(pathOut.getFileName().toString(), date);
		results.set("block size", blockSize);
		results.set("superblock size", superblockSize);
		
		try {
			if (vmdkMode) {
				scanVMDK(results);
			} else {
				scanDirectory(results);
			}
		} catch (IOException e) {
			System.err.println("A filesystem IO error ocurred.");
			System.exit(1);
		}
		
		return results;
	}
	
	/**
	 * Scan a VMDK.
	 * 
	 * @param res Results object in which to save the results.
	 * @throws IOException if an IO error occurs.
	 */
	private void scanVMDK(Results res) throws IOException {
		scanFile(pathIn, null, res);
	}
	
	/**
	 * Scan a directory.
	 * 
	 * @param res Results object in which to save the results.
	 * @throws IOException if file access failed.
	 */
	private void scanDirectory(Results res) throws IOException {
		Stream<Path> fileStream = Files.walk(pathIn);
		Iterator<Path> it = fileStream.iterator();
		
		
		
		fileStream.close();
	}
	
	/**
	 * Scan a file.
	 * 
	 * @param p1 Primary file to scan.
	 * @param p2 If scanning p1 reaches the end of the file in the middle of a superblock,
	 *           scanning continues with the beginning of p2 until the superblock is filled.
	 *           If null, the superblock is terminated prematurely.
	 * @param res Results object in which to save the results.
	 * @throws IOException if file access failed.
	 */
	private void scanFile(Path p1, Path p2, Results res) throws IOException {
		BufferedInputStream bs = new BufferedInputStream(Files.newInputStream(p1), buffSize);
		
		bs.close();
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
		} else if (Files.isDirectory(path)) {
			return true;
		} else {
			return isVMDK(path);
		}
	}
	
	/**
	 * Check if path is a valid virtual disk file.
	 * 
	 * @param path Path to verify.
	 * @return True if path is a valid virtual disk file.
	 */
	private static Boolean isVMDK(Path path) {
		if (path == null) {
			return false;
		} else {
			String[] partials = path.getFileName().toString().split("\\.(?=\\w+$)");
			// Short-circuits.
			return (partials.length == 2
					&& Arrays.asList(VALID_EXTENSIONS).contains(partials[1].toLowerCase()));
		}
	}
	
	/**
	 * Print help message.
	 * 
	 * @param custom a custom message to print
	 */
	private static void printHelp(String custom) {
		System.out.format(
				"Usage: CompScan [-h] [--vmdk] [--rate MB_PER_SEC] [--buffer-size BUFFER_SIZE]%n"
				+ "pathIn pathOut name blockSize superblockSize format%n"
				+ "Positional Arguments%n"
			    + "         pathIn            path to the dataset%n"
				+ "         pathOut           where to save the output%n"
				+ "         blockSize         bytes per block%n"
			    + "         superblockSize    bytes per superblock%n"
				+ "         format            compression scheme to use%n"
			    + "Optional Arguments%n"
				+ "         -h, --help        print this help message%n"
				+ "         --vmdk            flag to enable per-VMDK reporting instead of aggregate%n"
				+ "         --rate MB_PER_SEC maximum MB/sec we're allowed to read%n"
				+ "         --buffer-size BUFFER_SIZE     set the read buffer size in bytes%n"
			    );
		// Short-circuits.
		if (custom != null && custom.length() > 0) {
			System.out.format("%n" + custom + "%n");
		}
	}

	/**
	 * Main method.
	 * 
	 * @param args CLI arguments.
	 */
	public static void main(String[] args) {
		CompScan cs = null;
		try {
			cs = new CompScan(args);
		} catch (IllegalArgumentException ex) {
			printHelp(ex.getMessage());
		}
		if (cs == null) {
			System.exit(1);
		}
		
		Results results = cs.run();
		System.out.println(results.toString());
	}
	
	/**
	 * A simple subclass for encapsulating test results.
	 */
	private class Results {
		private final String[] KEYS = {
				"block size",
				"superblock size",
				"bytes read",
				"compressed blocks (raw)",
				"compressed blocks (sum, rounded-up)"
		};
		private Map<String, Long> map;
		private String name;
		private String timestamp;
		
		/**
		 * Create a new Results object.
		 * 
		 * @param name Name to assign to the resulting data set.
		 * @param date Date object from which to generate the timestamp.
		 */
		private Results(String name, Date date) {
			this.name = name;
			this.timestamp = new SimpleDateFormat("MM/dd/yyyy KK:mm:ss a Z").format(date);
			// Uses a LinkedHashMap to preserve insertion order.
			map = new LinkedHashMap<>();
			for (String s : KEYS) {
				map.put(s, 0L);
			}
		}
		
		/**
		 * Set the value for a given map key.
		 * 
		 * @param k Map key to update.
		 * @param v Value to assign to the key.
		 */
		public void set(String k, long v) {
			if (!map.containsKey(k)) {
				throw new IllegalArgumentException("Invalid key \"" + k + "\".");
			}
			map.put(k, v);
		}
		
		@Override
		public String toString() {
			List<String> lines = 
					map.entrySet()
					.stream()
					.map(e -> String.format("%1$s,%2$d", e.getKey(), e.getValue()))
					.collect(Collectors.toList());
			lines.add(0, "name," + name);
			lines.add(1, "timestamp," + timestamp);
			String s = String.format(String.join("%n", lines));
			return s;
		}
	}
}
