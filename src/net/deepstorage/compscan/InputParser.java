/**
 * 
 */
package net.deepstorage.compscan;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.ListIterator;
import java.util.Set;

import net.deepstorage.compscan.CompScan.ScanMode;

/**
 * @author user1
 *
 */
public class InputParser {
	// Expected number of arguments.
	public static final int MIN_ARGS = 5;
	// Positional arguments.
	public static final String[] POSITIONAL_ARGS = {
		"pathIn",
		"pathOut",
		"blockSize",
		"superblockSize",
		"formatString"
	};
	
	private CompScan compScan;
	
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
	 * Constructor.
	 * 
	 * @param compScan CompScan instance to configure.
	 */
	public InputParser(CompScan compScan) {
		this.compScan = compScan;
		scanMode = compScan.getScanMode();
		buffSize = compScan.getBuffSize();
		ioRate = compScan.getIORate();
	}
	
	
	/**
	 * Parse CLI arguments. Deliberately package-private since it will be called by CompScan.
	 * 
	 * @param args Arguments to parse.
	 * @throws IllegalArgumentException if an argument is invalid or unrecognized.
	 */
	void parse(String[] args) throws IllegalArgumentException {
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
		
		if (scanMode == ScanMode.FILE && !isVMDK(pathIn)) {
			throw new IllegalArgumentException(
					String.format(
							"VMDK mode specified but \"%1$d\" does not appear to be a "
							+ "valid .vhd, .vhdx, or .vmdk.",
							pathIn));
		}
		
		compScan.setup(buffSize, ioRate, pathIn, pathOut, blockSize, superblockSize, scanMode, formatString, compressor);
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
		case "formatString":
			formatString = arg;
			try {
				compressor = getCompressor(formatString);
			} catch (IllegalArgumentException ex) {
				throw ex;
			}
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
			CompScan.printHelp();
			System.exit(0);
		break;
		// VMDK.
		case "--vmdk":
			scanMode = ScanMode.FILE;
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
								CompScan.UNLIMITED));
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
								CompScan.DEFAULT_BUFFSIZE));
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
	 * Get the Compressor for the specified format string.
	 * 
	 * @param formatString Name of the compression scheme to retrieve.
	 * @return Compressor for the format string.
	 * @throws IllegalArgumentException if the Compressor for the format string does not exist.
	 */
	private Compressor getCompressor(String formatString) throws IllegalArgumentException {
		class WrongClassException extends Exception {
			WrongClassException(String message) {
				super(message);
			}
		}
		
		String compressName = String.join(".", getClass().getPackage().getName(), CompScan.COMPRESSION_SUBPACKAGE, formatString);
		
		try {
			Class<?> compressor = Class.forName(compressName);
			Set<Class<?>> classSet = getAncestors(compressor);
			if (!classSet.contains(Compressor.class.getClass())) {
				throw new WrongClassException(
						String.format(
								"Class \"%1$s\" found for format string \"%2$s\" but is not a valid Compressor.",
								compressor.getClass().getName(), formatString));
			}
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(
					String.format(
							"Unable to locate Compressor for compression format \"%s\".", formatString));
		} catch (WrongClassException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		
		System.out.println(String.format("Using Compressor \"%s\".", compressName));
		return (Compressor) compressor;
	}
	
	/**
	 * Traverse the class hierarchy to get all superclasses of a specified child class.
	 * 
	 * @param child Leaf class for which to get the superclasses.
	 * @return Set containing all superclasses of child.
	 */
	private Set<Class<?>> getAncestors(Class<?> child) {
		Set<Class<?>> classSet = new HashSet<>();
		Class<?> c = child.getClass();
		while (c != null) {
			classSet.add(c);
			c = c.getSuperclass();
		}
		return classSet;
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
					&& Arrays.asList(CompScan.VALID_EXTENSIONS).contains(partials[1].toLowerCase()));
		}
	}
}
