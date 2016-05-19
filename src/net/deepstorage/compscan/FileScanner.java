/**
 * 
 */
package net.deepstorage.compscan;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import net.deepstorage.compscan.CompScan.Results;
import net.deepstorage.compscan.CompScan.ScanMode;
import net.deepstorage.compscan.Compressor.BufferLengthException;
import net.deepstorage.compscan.Compressor.CompressionInfo;

/**
 * The FileScanner class abstracts the necessary behavior for walking a file tree.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class FileScanner {
	private Path root;
	private int bufferSize;
	private Compressor compressor;
	private Results results;
	private int superblockSize;
	private double ioRate;
	
	/**
	 * Constructor.
	 * 
	 * @param root Root of the datastore to scan.
	 * @param bufferSize Size of the internal read buffer.
	 * @param ioRate Maximum IO rate in MB/s to throttle the scanning.
	 * @param compressor Compressor to use.
	 * @param results Results object to update with the scan data.
	 */
	public FileScanner(Path root, int bufferSize, double ioRate, Compressor compressor, Results results) {
		this.root = root;
		this.bufferSize = bufferSize;
		
		this.compressor = compressor;
		this.results = results;
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
	public void scan() throws IOException, BufferLengthException, NoNextFileException {
		try (FileWalkerStream fws = new FileWalkerStream(new FileWalker(root), bufferSize, ioRate)) {
			if (!fws.hasMore()) {
				throw new NoNextFileException(
						String.format(
								"FileWalkerStream with root \"%s\" contains no scannable data.", root));
			}
			scanStream(fws, results);
		} catch (IOException ex) {
			throw ex;
		}
	}
	
	/**
	 * Run scan in VMDK mode.
	 * 
	 * @param fileResults List of Results in which to store the new scan results.
	 * @param totals Results object to update with totals data.
	 * @throws IOException if an IO error occurs.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 * @throws NoNextFileException if the file root contains no VMDKs.
	 */
	public void scanVMDKMode(List<Results> fileResults, Results totals) throws IOException, BufferLengthException, NoNextFileException {
		try (FileWalker fw = new FileWalker(root, ScanMode.VMDK)) {
			if (!fw.hasNext()) {
				throw new NoNextFileException(
						String.format(
								"FileWalker opened in VMDK mode with root \"%s\" but contains no VMDKs.", root));
			}
			
			while (fw.hasNext()) {
				Path f = fw.next();
				Results r = new Results(f.toString(), results.getTimestamp());
				FileWalkerStream fws = new FileWalkerStream(new FileWalker(f, ScanMode.NORMAL), bufferSize, ioRate);
				scanStream(fws, r);
				fileResults.add(r);
				totals.feedOtherResults(r);
			}
		} catch (IOException ex) {
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
		while (fws.hasMore()) {
			byte[] buffer = fws.getBytes();
			// Since the read buffer is enforced to be an even multiple of the superblock size, we
			// can use a simple iteration to feed the data to the compressor.
			for (int i = 0, j = superblockSize; j <= buffer.length; i += superblockSize, j += superblockSize) {
				byte[] segment = Arrays.copyOfRange(buffer, i, j);
				CompressionInfo ci = compressor.feedData(segment);
				r.feedCompressionInfo(ci);
				r.set("files read", fws.getFilesRead());
			}
		}
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
