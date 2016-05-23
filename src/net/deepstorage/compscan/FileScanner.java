/**
 * 
 */
package net.deepstorage.compscan;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
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
	private Results totals;
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
		this.totals = results;
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
			scanStream(fws, totals);
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
			scanBuffer(buffer, r);
			r.set("files read", fws.getFilesRead());
		}
	}
	
	/**
	 * Run scan in VMDK mode.
	 * 
	 * @param fileResults List of Results in which to store the new scan results.
	 * @throws IOException if an IO error occurs.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 * @throws NoNextFileException if the file root contains no VMDKs.
	 */
	public void scanVMDKMode(List<Results> fileResults) throws IOException, BufferLengthException, NoNextFileException {
		try (FileWalker fw = new FileWalker(root, ScanMode.VMDK)) {
			if (!fw.hasNext()) {
				throw new NoNextFileException(
						String.format(
								"FileWalker opened in VMDK mode with root \"%s\" but contains no VMDKs.", root));
			}
			
			while (fw.hasNext()) {
				Path f = fw.next();
				Results r = new Results(f.toString(), totals.getTimestamp());
				scanFile(f, r);
				fileResults.add(r);
			}
		} catch (IOException ex) {
			throw ex;
		}
	}
	
	/**
	 * Scan a single file.
	 * 
	 * @param f Path to the file to scan.
	 * @param r Results object to update with the scan data.
	 * @throws IOException if an IO error occurs.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 */
	private void scanFile(Path f, Results r) throws IOException, BufferLengthException {
		if (f == null || r == null) {
			return;
		}
		
		BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(f), bufferSize);
		byte[] buffer = new byte[bufferSize];
		
		totals.incrementFilesRead();
		
		int bytesRead = 0;
		
		while (bis.available() > 0) {
			bytesRead = bis.read(buffer);
			// Nothing left to read. Abort.
			if (bytesRead <= 0) {
				break;
			// Zero out rest of buffer if necessary.
			} else if (bytesRead < bufferSize) {
				for (int i = bytesRead; i < buffer.length; i++) {
					buffer[i] = 0x0;
				}
			}
			Results intermediate = new Results(f.toString(), r.getTimestamp());
			scanBuffer(buffer, intermediate);
			r.feedOtherResults(intermediate);
			totals.feedOtherResults(intermediate);
		};
	}
	
	/**
	 * Scan a data buffer by splitting it into superblocks. The buffer size is automatically rounded up
	 * to the next even multiple of the superblock size, making this easy.
	 * 
	 * @param b Data buffer to scan. Must have length == bufferSize.
	 * @param r Results object to update with scan results.
	 * @throws BufferLengthException if the buffers are the wrong size.
	 */
	private void scanBuffer(byte[] b, Results r) throws BufferLengthException {
		if (b.length != bufferSize) {
			throw new BufferLengthException(
					String.format(
							"Input buffer size is %1$d but data buffer provided is size %2$d.",
							bufferSize, b.length));
		}
		for (int i = 0, j = superblockSize; j <= b.length; i += superblockSize, j += superblockSize) {
			byte[] segment = Arrays.copyOfRange(b, i, j);
			scanSuperblock(segment, r);
		}
	}
	
	/**
	 * Scan a single superblock of data.
	 * 
	 * @param segment Data buffer to scan. Must be one superblock in length.
	 * @param r Results object to update with scan results.
	 * @throws BufferLengthException if the buffers are the wrong size.
	 */
	private void scanSuperblock(byte[] segment, Results r) throws BufferLengthException {
		if (segment.length != superblockSize) {
			throw new BufferLengthException(
					String.format(
							"Superblock size is %1$d but data buffer provided is size %2$d.",
							superblockSize, segment.length));
		}
		CompressionInfo ci = compressor.feedData(segment);
		r.feedCompressionInfo(ci);
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
