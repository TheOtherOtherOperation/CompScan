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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.deepstorage.compscan.Compressor.BufferLengthException;
import net.deepstorage.compscan.Compressor.CompressionInfo;
import net.deepstorage.compscan.FileScanner.NoNextFileException;

/**
 * CompScan's main class.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class CompScan {
	// Valid file extensions.
	public static final String[] VALID_EXTENSIONS = {
			"txt",
			"vhd",
			"vhdx",
			"vmdk"
	};
	// Symbolic constant representing unthrottled IO rate.
	public static final double UNLIMITED = 0.0;
	// Subpackage prefix for the compression package.
	public static final String COMPRESSION_SUBPACKAGE = "compress";
	// Symbolic constant for 1 million bytes (1 MB), the default input buffer size.
	public static final int ONE_MB = 1_000_000;
	
	private final Date date;
	
	private boolean setupLock;
	
	private double ioRate;
	private Path pathIn;
	private Path pathOut;
	private ScanMode scanMode;
	private int blockSize;
	private int superblockSize;
	private int bufferSize;
	private boolean overwriteOK;
	private Compressor compressor;
	
	/**
	 * Default constructor.
	 * 
	 * @param args CLI arguments.
	 */
	private CompScan(String[] args) throws IllegalArgumentException {
		ioRate = UNLIMITED;
		pathIn = null;
		pathOut = null;
		scanMode = ScanMode.NORMAL;
		blockSize = 0;
		superblockSize = 0;
		bufferSize = ONE_MB;
		overwriteOK = false;
		compressor = null;
		
		setupLock = false;
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
	 * @throws Exception if called more than once.
	 */
	void setup(double ioRate, Path pathIn, Path pathOut, ScanMode scanMode, int blockSize, int superblockSize,
			int bufferSize, boolean overwriteOK, Compressor compressor) {
		if (setupLock) {
			System.err.println("CompScan.setup cannot be called more than once.");
			System.exit(1);
		}
		this.ioRate = ioRate;
		this.pathIn = pathIn;
		this.pathOut = pathOut;
		this.scanMode = scanMode;
		this.blockSize = blockSize;
		this.superblockSize = superblockSize;
		this.bufferSize = bufferSize;
		this.overwriteOK = overwriteOK;
		this.compressor = compressor;
		setupLock = true;
	}
	
	/**
	 * Run scan.
	 */
	private void run() {
		System.out.format("Starting run.%n%n");

		Results results = new Results(pathOut.getFileName().toString(), date);
		results.set("block size", blockSize);
		results.set("superblock size", superblockSize);
		
		ConsoleDisplayThread cdt = new ConsoleDisplayThread(results);

		try {
			FileScanner fs = new FileScanner(pathIn, bufferSize, ioRate, compressor, results);
			cdt.start();
			fs.scan();
		} catch (IOException e) {
			System.err.format("A filesystem IO error ocurred.%n%n");
			e.printStackTrace();
			System.exit(1);
		} catch (BufferLengthException e) {
			System.err.format("The input buffer is the wrong size.%n%n");
			e.printStackTrace();
			System.exit(1);
		} catch (NoNextFileException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		
		cdt.interrupt();
		try {
			cdt.join();
		} catch (InterruptedException e) {
			// Nothing to do.
		}
		
		// Save results.
		try {
			String pathOut = writeResults(results.toString());
			System.out.println(
					String.format(
							"%n--> Output saved as \"%s\".%n", pathOut));
		} catch (IOException e) {
			System.err.println("Unable to save output.");
			e.printStackTrace();
		} finally {
			System.out.println(results.toString());
		}
	}
	
	/**
	 * Run VMDK-mode scan.
	 */
	private void runVMDKMode() {
		System.out.format("Starting run.%n%n");
		
		Results totals = new Results(pathOut.getFileName().toString(), date);
		totals.set("block size", blockSize);
		totals.set("superblock size", superblockSize);
		
		List<Results> allResults = new LinkedList<>();
		
		ConsoleDisplayThread cdt = new ConsoleDisplayThread(totals);
		
		try {
			FileScanner fs = new FileScanner(pathIn, bufferSize, ioRate, compressor, totals);
			cdt.start();
			fs.scanVMDKMode(allResults);
		} catch (IOException e) {
			System.err.format("A filesystem IO error ocurred.%n%n");
			e.printStackTrace();
			System.exit(1);
		} catch (BufferLengthException e) {
			System.err.format("The input buffer is the wrong size.%n%n");
			e.printStackTrace();
			System.exit(1);
		} catch (NoNextFileException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		
		cdt.interrupt();
		try {
			cdt.join();
		} catch (InterruptedException e) {
			// Nothing to do.
		}
		
		// Save results.
		String resultString = makeVMDKResultString(allResults, totals);
		try {
			String pathOut = writeResults(resultString);
			System.out.println(
					String.format(
							"%n--> Output saved as \"%s\".%n", pathOut));
		} catch (IOException e) {
			System.err.println("Unable to save output.");
			e.printStackTrace();
		} finally {
			System.out.println(resultString);
		}
	}
	
	/**
	 * Convert VMDK scan results into a string for saving to the output CSV.
	 * 
	 * @param allResults List of Results objects containing the intermediate results.
	 * @param totals Results object containing the aggregate results.
	 */
	public String makeVMDKResultString(List<Results> allResults, Results totals) {
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
	
	/**
	 * Save the results to a CSV file.
	 * 
	 * @param resultString Results string to save.
	 * @return The actual path where the file was saved. Might not be the same as pathOut if pathOut already exists.
	 * @throws IOException if an IO error occurred.
	 */
	public String writeResults(String resultString) throws IOException {
		Path writePath = pathOut;
		if (Files.exists(writePath) && !overwriteOK) {
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
			bw.write(resultString);
		} catch (IOException e) {
			throw e;
		}
		return writePath.toString();
	}
	
	/**
	 * Getter for ioRate.
	 * 
	 * @return The IOPS limit (0 = UNLIMITED).
	 */
	public double getIORate() {
		return ioRate;
	}
	
	/**
	 * Print help message.
	 * 
	 * @param custom a custom message to print
	 */
	public static void printHelp(String custom) {
		System.out.format(
				"Usage: CompScan [-h] [--help] [--vmdk] [--overwrite] [--rate MB_PER_SEC] [--buffer-size BUFFER_SIZE]%n"
			    + "                pathIn pathOut blockSize superblockSize format%n"
				+ "Positional Arguments%n"
			    + "         pathIn            path to the dataset%n"
				+ "         pathOut           where to save the output%n"
				+ "         blockSize         bytes per block%n"
			    + "         superblockSize    bytes per superblock%n"
				+ "         formatString      compression format to use%n"
			    + "Optional Arguments%n"
				+ "         -h, --help        print this help message%n"
			    + "         --vmdk            whether to report individual virtual disks separately%n"
			    + "         --overwrite       whether overwriting the output file is allowed%n"
				+ "         --rate MB_PER_SEC maximum MB/sec we're allowed to read%n"
			    + "         --buffer-size BUFFER_SIZE size of the internal read buffer%n"
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
		
		if (cs.scanMode == ScanMode.VMDK) {
			cs.runVMDKMode();
		} else {
			cs.run();
		}
	}
	
	/**
	 * Nested enumeration for tracking the scan mode.
	 */
	public static enum ScanMode {
		NORMAL, VMDK;
	}
	
	/**
	 * A simple subclass for encapsulating test results.
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
		
		/**
		 * Convenience constructor for creating a new Results object from a name
		 * and Date object.
		 * 
		 * @param name Name to assign to the resulting data set.
		 * @param date Date object from which to generate the timestamp.
		 */
		public Results(String name, Date date) {
			this(name, new SimpleDateFormat("MM/dd/yyyy KK:mm:ss a Z").format(date));			
		}
		
		/**
		 * Create a new Results object.
		 * 
		 * @param name Name to assign to the resulting data set.
		 * @param timestamp Formatted timestamp string to use.
		 */
		public Results(String name, String timestamp) {
			this.name = name;
			this.timestamp = timestamp;
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
		
		/**
		 * Get the value for a given map key.
		 * 
		 * @param k Map key to retrieve.
		 * @return Value assigned to the key.
		 */
		public long get(String k) {
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
		 * Feed a CompressionInfo object into the Results to update the counters.
		 * 
		 * @param ci CompressionInfo whose data should be added to the results.
		 */
		public void feedCompressionInfo(CompressionInfo ci) {
			addTo("bytes read", ci.bytesRead);
			addTo("blocks read", ci.blocksRead);
			addTo("superblocks read", ci.superblocksRead);
			addTo("compressed bytes", ci.compressedBytes);
			addTo("compressed blocks", ci.compressedBlocks);
			addTo("actual bytes needed", ci.actualBytes);
		}
		
		/**
		 * Feed another Results object into the Results to update the counters.
		 */
		public void feedOtherResults(Results r) {
			addTo("files read", r.get("files read"));
			addTo("bytes read", r.get("bytes read"));
			addTo("blocks read", r.get("blocks read"));
			addTo("superblocks read", r.get("superblocks read"));
			addTo("compressed bytes", r.get("compressed bytes"));
			addTo("compressed blocks", r.get("compressed blocks"));
			addTo("actual bytes needed", r.get("actual bytes needed"));
		}
		
		/**
		 * Increment the files read counter.
		 */
		public void incrementFilesRead() {
			set("files read", get("files read") + 1);
		}
		
		/**
		 * Get the raw compression factor.
		 * 
		 * @return Compressed bytes / bytes read.
		 */
		public double getRawCompressionFactor() {
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
		public double getSuperblockCompressionFactor() {
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
		public String makeHeadingString() {
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
		public String makeValueString() {
			List<String> values = new LinkedList<>(
					map.values()
					.stream()
					.map(v -> String.valueOf(v))
					.collect(Collectors.toList()));
			values.add(0, name);
			values.add(1, timestamp);
			values.add(String.valueOf(getRawCompressionFactor()));
			values.add(String.valueOf(getSuperblockCompressionFactor()));
			return String.join(",",  values);
		}
		
		@Override
		public String toString() {			
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
	}
}
