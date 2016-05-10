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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * CompScan's main class.
 * 
 * @author Ramon A. Lovato (ramonalovato.com)
 * @version 1.0
 */
public class CompScan {
	// Expected number of arguments.
	public static final int MIN_ARGS = 4;
	// Valid file extensions.
	public static final String[] VALID_EXTENSIONS = {
			".vhd",
			".vhdx",
			".vmdk"
	};
	
	private String name;
	private Date date;
	private Path pathIn;
	private int blockSize;
	private int superblockSize;
	private String format;
	
	/**
	 * Default constructor.
	 */
	private CompScan(String[] args) throws IllegalArgumentException {
		date = Calendar.getInstance().getTime();
		parseArgs(args);
	}
	
	/**
	 * Parse CLI arguments.
	 */
	private void parseArgs(String[] args) throws IllegalArgumentException {
		if (args.length < MIN_ARGS) {
			throw new IllegalArgumentException(
					String.format(
							"Invalid number of arguments -- %1$d given, %2$d expected.",
							args.length, MIN_ARGS));
		}
		pathIn = Paths.get(args[0]);
		if (!Files.exists(pathIn)) {
			throw new IllegalArgumentException(
					String.format("Input path \"%1$s\" does not exist.", pathIn));
		} else if (!isValidPath(pathIn)) {
			throw new IllegalArgumentException(
					String.format("Input path must be a directory, .vhd, .vhdx, or .vmdk "
							+ "-- \"%1$s\" given.",
							args[0]));
		}
		try {
			blockSize = Integer.parseInt(args[1]);
			if (blockSize < 1) {
				throw new NumberFormatException();
			}
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException(
					String.format("Block size must be a positive integer -- \"%1$s\" given.",
							args[1]));
		}
		try {
			superblockSize = Integer.parseInt(args[2]);
			if (superblockSize < 1) {
				throw new NumberFormatException();
			}
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException(
					String.format("Superblock size must be a positive integer -- \"%1$s\" given.",
							args[2]));
		}
		format = args[3];
	}
	
	/**
	 * Run scan.
	 */
	private Results run() {
		Results results = new Results(name, date);
		results.set("block size", blockSize);
		results.set("superblock size", superblockSize);
		
		// TODO
		
		return results;
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
			String[] partials = path.toString().split("\\.(?=\\w+$)");
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
				"Usage: CompScan [--vmdk] pathIn blockSize superblockSize format%n"
				+ "         --vmdk            flag to enable per-VMDK reporting instead of aggregate%n"
			    + "         pathIn            path to the dataset%n"
				+ "         blockSize         bytes per block%n"
			    + "         superblockSize    bytes per superblock%n"
				+ "         format            compression scheme to use%n");
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
		
		private Results(String name, Date date) {
			this.name = name;
			this.timestamp = new SimpleDateFormat("MM/dd/yyyy KK:mm:ss a Z").format(date);
			// Uses a TreeMap to preserve order.
			map = new TreeMap<>();
			for (String s : KEYS) {
				map.put(s, 0L);
			}
		}
		
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
