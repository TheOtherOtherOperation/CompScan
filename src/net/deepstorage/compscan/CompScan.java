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

/**
 * CompScan's main class.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class CompScan {
	// Valid file extensions.
	public static final String[] VALID_EXTENSIONS = {
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
	
	private boolean setupLock;
	private Date date;
	
	private double ioRate;
	private Path pathIn;
	private Path pathOut;
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
		setupLock = false;
		date = Calendar.getInstance().getTime();
		ioRate = UNLIMITED;
		bufferSize = ONE_MB;
		overwriteOK = false;
		InputParser ip = new InputParser(this);
		ip.parse(args);
	}

	/**
	 * Allows other package members, notably the InputParser, to set fields. Deliberately package-private.
	 * 
	 * @param ioRate IOPS limit.
	 * @param pathIn Input path.
	 * @param pathOut Output path.
	 * @param blockSize Block size in bytes.
	 * @param superblockSize Superblock size in bytes.
	 * @param bufferSize Size of the internal read buffer.
	 * @param overwriteOK Whether it's allowed to overwrite the output file.
	 * @param compressor Compressor associated with formatString.
	 * @throws Exception if called more than once.
	 */
	void setup(double ioRate, Path pathIn, Path pathOut, int blockSize, int superblockSize,
			int bufferSize, boolean overwriteOK, Compressor compressor) {
		if (setupLock) {
			System.err.println("CompScan.setup cannot be called more than once.");
			System.exit(1);
		}
		this.ioRate = ioRate;
		this.pathIn = pathIn;
		this.pathOut = pathOut;
		this.blockSize = blockSize;
		this.superblockSize = superblockSize;
		this.bufferSize = bufferSize;
		this.overwriteOK = overwriteOK;
		this.compressor = compressor;
		setupLock = true;
	}
	
	/**
	 * Run scan.
	 * 
	 * @return Results object containing the test results.
	 */
	private Results run() {
		System.out.format("Starting run.%n%n");

		Results results = new Results(pathOut.getFileName().toString(), date);
		results.set("block size", blockSize);
		results.set("superblock size", superblockSize);
		
		ConsoleDisplayThread cdt = new ConsoleDisplayThread(results);

		try {
			FileScanner fc = new FileScanner(pathIn, bufferSize, ioRate, compressor, results);
			cdt.start();
			fc.scan();
		} catch (IOException e) {
			System.err.format("A filesystem IO error ocurred.%n%n");
			e.printStackTrace();
			System.exit(1);
		} catch (BufferLengthException e) {
			System.err.format("The input buffer is the wrong size.%n%n");
			e.printStackTrace();
			System.exit(1);
		}
		
		cdt.interrupt();
		try {
			cdt.join();
		} catch (InterruptedException e) {
			// Nothing to do.
		}
		
		return results;
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
			bw.write(results.toString());
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
				"Usage: CompScan [-h] [--help] [--overwrite] [--rate MB_PER_SEC] [--buffer-size BUFFER_SIZE]%n"
			    + "                pathIn pathOut name blockSize superblockSize format%n"
				+ "Positional Arguments%n"
			    + "         pathIn            path to the dataset%n"
				+ "         pathOut           where to save the output%n"
				+ "         blockSize         bytes per block%n"
			    + "         superblockSize    bytes per superblock%n"
				+ "         formatString      compression format to use%n"
			    + "Optional Arguments%n"
				+ "         -h, --help        print this help message%n"
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
		
		Results results = cs.run();
		try {
			String pathOut = cs.writeResults(results);
			System.out.println(
					String.format(
							"Output saved as \"%s\".%n", pathOut));
		} catch (IOException e) {
			System.err.println("Unable to save output.");
			e.printStackTrace();
		} finally {
			System.out.println(results.toString());
		}
	}
	
	/**
	 * A simple subclass for encapsulating test results.
	 */
	public class Results {
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
		 * Feed a CompressionInfo object into the Results to update the counters.
		 * 
		 * @param ci CompressionInfo whose data should be added to the results.
		 */
		public void feedCompressionInfo(Compressor.CompressionInfo ci) {
			addTo("bytes read", ci.bytesRead);
			addTo("blocks read", ci.blocksRead);
			addTo("superblocks read", ci.superblocksRead);
			addTo("compressed bytes", ci.compressedBytes);
			addTo("compressed blocks", ci.compressedBlocks);
			addTo("actual bytes needed", ci.actualBytes);
		}
		
		/**
		 * Get the raw compression factor.
		 * 
		 * @return Compressed bytes / bytes read.
		 */
		public double getRawCompressionFactor() {
			long bytesRead = map.get("bytes read");
			if (bytesRead == 0) {
				return 0;
			}
			return (double) map.get("compressed bytes") / (double) bytesRead;
		}
		
		/**
		 * Get the superblock compression factor.
		 * 
		 * @return Actual bytes needed / bytes read.
		 */
		public double getSuperblockCompressionFactor() {
			long bytesRead = map.get("bytes read");
			if (bytesRead == 0) {
				return 0;
			}
			return (double) map.get("actual bytes needed") / (double) bytesRead;
		}
		
		@Override
		public String toString() {
			List<String> headings = new LinkedList<>(map.keySet());
			List<String> values = new LinkedList<>(
					map.values()
					.stream()
					.map(v -> String.valueOf(v))
					.collect(Collectors.toList()));
			headings.add(0, "name");
			values.add(0, name);
			headings.add(1, "timestamp");
			values.add(1, timestamp);
			headings.add("raw compression factor");
			values.add(String.valueOf(getRawCompressionFactor()));
			headings.add("superblock compression factor");
			values.add(String.valueOf(getSuperblockCompressionFactor()));
			
			String headerString = String.join(",", headings);
			String valueString = String.join(",", values);
			
			return headerString + System.lineSeparator() + valueString;
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
	}
}
