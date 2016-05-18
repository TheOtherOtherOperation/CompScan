/**
 * 
 */
package net.deepstorage.compscan;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import net.deepstorage.compscan.Compressor.BufferLengthException;

/**
 * The FileScanner class abstracts the necessary behavior for walking a file tree.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class FileScanner {
	private Path root;
	private byte[] buffer;
	private Compressor compressor;
	private CompScan.Results results;
	private int superblockSize;
	private int delayMS;
	
	/**
	 * Constructor.
	 * 
	 * @param root Root of the datastore to scan.
	 * @param bufferSize Size of the internal read buffer.
	 * @param ioRate Maximum IO rate in MB/s to throttle the scanning.
	 * @param compressor Compressor to use.
	 * @param results Results object to update with the scan data.
	 */
	public FileScanner(Path root, int bufferSize, double ioRate, Compressor compressor, CompScan.Results results) {
		this.root = root;
		buffer = new byte[bufferSize];
		this.compressor = compressor;
		this.results = results;
		superblockSize = compressor.getSuperblockSize();
		
		if (ioRate == CompScan.UNLIMITED) {
			delayMS = 0;
		} else {
			double buffsPerSec = (CompScan.ONE_MB / bufferSize) * ioRate;
			delayMS = (int) (1000.0 / buffsPerSec);
		}
		
		clearBuffer();
	}
	
	/**
	 * Run scan of the appropriate mode.
	 * 
	 * @throws IOException if an IO error occurs.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 */
	public void scan() throws IOException, BufferLengthException {
		try (FileWalker walker = new FileWalker(root)) {
			if (!walker.hasNext()) {
				throw new IOException(
						String.format(
								"FileWalker root \"%s\" contains no regular files.", root));
			}
			while (walker.hasNext()) {
				Path current = walker.next();
				scanFile(current, walker);
			}
		} catch (IOException ex) {
			throw ex;
		}
	}
	
	/**
	 * A helper function that reads bytes from a BufferedInputStream into a buffer and throttles
	 * the read rate if appropriate.
	 * @throws IOException 
	 */
	private int readThrottled(BufferedInputStream bs, byte[] buffer, int start, int end) throws IOException {
		long initial = System.currentTimeMillis();
		int bytesRead = bs.read(buffer, start, end);
		long elapsed = System.currentTimeMillis() - initial;
		// This will always be false if there's no limit, since delayMS will be 0.
		if (elapsed < delayMS) {
			try {
				Thread.sleep(delayMS-elapsed);
			} catch (InterruptedException e) {
				// Do nothing, since we don't really care if it's interrupted.
			}
		}
		return bytesRead;
	}
	
	/**
	 * Scan files.
	 * 
	 * @param p Primary file to scan.
	 * @param w FileWalker for walking the directory.
	 * @throws IOException if file access failed.
	 * @throws BufferLengthException if the buffer is the wrong size.
	 */
	private void scanFile(Path p, FileWalker w) throws IOException, BufferLengthException {
		if (p == null) {
			return;
		}
		
		System.out.println("before1");
		BufferedInputStream bs = new BufferedInputStream(Files.newInputStream(p), buffer.length);
		while (bs.available() > 0) {
			int bytesRead = readThrottled(bs, buffer, 0, buffer.length);
			
			while (bytesRead < buffer.length) {
				if (w.hasLookAhead()) {
					BufferedInputStream bs2 = new BufferedInputStream(Files.newInputStream(w.lookAhead()));
					bytesRead += readThrottled(bs2, buffer, bytesRead, buffer.length - bytesRead);
					bs2.close();
				} else {
					clearBuffer(bytesRead, buffer.length);
					break;
				}
			}
			
			for (int i = 0; i < buffer.length; i += superblockSize) {
				byte[] segment = Arrays.copyOfRange(buffer, i, i + superblockSize);
				Compressor.CompressionInfo info = compressor.feedData(segment);
				results.feedCompressionInfo(info);
			}
		}
		System.out.println("after1");
		
		bs.close();
		results.set("files read", results.get("files read") + 1);
	}
	
	/**
	 * Clear the entire read buffer.
	 */
	private void clearBuffer() {
		clearBuffer(0, buffer.length);
	}
	
	/**
	 * Clear part of the read buffer.
	 * 
	 * @param start Index from which to start clearing.
	 * @param end Maximum index for clearing. End itself is not cleared.
	 */
	private void clearBuffer(int start, int end) {
		if (end < start) {
			return;
		}
		if (start < 0) {
			start = 0;
		}
		if (end > buffer.length) {
			end = buffer.length;
		}
		for (int i = start; i < end; i++) {
			buffer[i] = 0x0;
		}
	}
	
}
