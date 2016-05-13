/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.deepstorage.compscan.compress.*;

/**
 * CompScan's main class.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class CompScan {
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
	// Subpackage prefix for the compression package.
	public static final String COMPRESSION_SUBPACKAGE = "compress";
	
	private boolean setupLock;
	private Date date;
	
	private int buffSize;
	private int ioRate;
	private Path pathIn;
	private Path pathOut;
	private int blockSize;
	private int superblockSize;
	private String formatString;
	private ScanMode scanMode;
	private Compressor compressor;
	
	/**
	 * Default constructor.
	 * 
	 * @param args CLI arguments.
	 */
	private CompScan(String[] args) throws IllegalArgumentException {
		setupLock = false;
		date = Calendar.getInstance().getTime();
		scanMode = ScanMode.DIRECTORY;
		buffSize = DEFAULT_BUFFSIZE;
		ioRate = UNLIMITED;
		InputParser ip = new InputParser(this);
		ip.parse(args);
	}

	/**
	 * Allows other package members, notably the InputParser, to set fields. Deliberately package-private.
	 * 
	 * @param buffSize Input buffer size.
	 * @param ioRate IOPS limit.
	 * @param pathIn Input path.
	 * @param pathOut Output path.
	 * @param blockSize Block size in bytes.
	 * @param superblockSize Superblock size in bytes.
	 * @param scanMode Whether to scan a single VMDK or a directory.
	 * @param formatString Name of the compression scheme to use.
	 * @param compressor Compressor associated with formatString.
	 * @throws Exception if called more than once.
	 */
	void setup(int buffSize, int ioRate, Path pathIn, Path pathOut, int blockSize,int superblockSize,
			ScanMode scanMode, String formatString, Compressor compressor) {
		if (setupLock) {
			System.err.println("CompScan.setup cannot be called more than once.");
			System.exit(1);
		}
		this.buffSize = buffSize;
		this.ioRate = ioRate;
		this.pathIn = pathIn;
		this.pathOut = pathOut;
		this.blockSize = blockSize;
		this.superblockSize = superblockSize;
		this.formatString = formatString;
		this.scanMode = scanMode;
		this.compressor = compressor;
		setupLock = true;
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
			if (scanMode == ScanMode.FILE) {
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
	 * Save the results to a CSV file.
	 * 
	 * @param results Results to save.
	 * @return The actual path where the file was saved. Might not be the same as pathOut if pathOut already exists.
	 * @throws IOException if an IO error occurred.
	 */
	public String writeResults(Results results) throws IOException {
		Path writePath = pathOut;
		if (Files.exists(writePath)) {
			int i = 0;
			String[] partials = writePath.toString().split("\\.(?=\\w+$)");
			while (Files.exists(writePath)) {
				String pathString = (partials.length < 2 ?
						String.format("%1$s (%2$d).%3$s", partials[0], ++i, partials[1]) :
						String.format("%1$s %2$d", partials[0]));
				writePath = Paths.get(pathString);
			}
		}
		
		try (BufferedWriter bw = Files.newBufferedWriter(writePath)) {
			bw.write(results.toString());
		} catch (IOException e) {
			throw e;
		}
		return writePath.toString();
	}
	
	/**
	 * Getter for scanMode.
	 * 
	 * @return The ScanMode.
	 */
	public ScanMode getScanMode() {
		return scanMode;
	}
	
	/**
	 * Getter for buffSize.
	 * 
	 * @return The read buffer size in bytes.
	 */
	public int getBuffSize() {
		return buffSize;
	}
	
	/**
	 * Getter for ioRate.
	 * 
	 * @return The IOPS limit (0 = UNLIMITED).
	 */
	public int getIORate() {
		return ioRate;
	}
	
	/**
	 * Print help message.
	 * 
	 * @param custom a custom message to print
	 */
	public static void printHelp(String custom) {
		System.out.format(
				"Usage: CompScan [-h] [--vmdk] [--rate MB_PER_SEC] [--buffer-size BUFFER_SIZE]%n"
				+ "pathIn pathOut name blockSize superblockSize format%n"
				+ "Positional Arguments%n"
			    + "         pathIn            path to the dataset%n"
				+ "         pathOut           where to save the output%n"
				+ "         blockSize         bytes per block%n"
			    + "         superblockSize    bytes per superblock%n"
				+ "         formatString      compression format to use%n"
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
		try {
			String pathOut = cs.writeResults(results);
		} catch (IOException e) {
			System.err.println("Unable to save output.");
			e.printStackTrace();
		}
		System.out.println(results.toString());
	}
	
	/**
	 * A nested enum for symbolic constants to indicate single-file or directory mode.
	 */
	public static enum ScanMode {
		FILE, DIRECTORY;
	}
	
	/**
	 * A simple subclass for encapsulating test results.
	 */
	private class Results {
		private final String[] KEYS = {
				"block size",
				"superblock size",
				"bytes read",
				"blocks read",
				"superblocks read",
				"compressed bytes",
				"compressed blocks",
				"actual bytes needed"
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
