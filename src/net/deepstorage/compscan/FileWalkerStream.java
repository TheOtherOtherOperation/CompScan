/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * A filesystem walker data stream for CompScan.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class FileWalkerStream implements AutoCloseable {
	private final FileWalker walker;
	private final int blockSize;
	private final int bufferSize;
	private BufferedInputStream bs;
	private int delayMS;
	private boolean noStep;
	
	/**
	 * Constructor.
	 * 
	 * @param walker The FileWalker that backs this stream.
	 * @param blockSize Size of one IO block.
	 * @param bufferSize Size of the internal read buffer.
	 * @param ioRate Maximum MB/sec we're allowed to perform.
	 * @param noStep Prohibit stepping to the next file (single-file-only mode).
	 * @throws IOException if the underlying reader failed.
	 */
	public FileWalkerStream(FileWalker walker, int blockSize, int bufferSize, double ioRate, boolean noStep) throws IOException {
		this.walker = walker;
		this.blockSize = blockSize;
		this.bufferSize = bufferSize;
		bs = null;
		if (ioRate == CompScan.UNLIMITED) {
			delayMS = 0;
		} else {
			double buffsPerSec = (CompScan.ONE_MB / bufferSize) * ioRate;
			delayMS = (int) (1000.0 / buffsPerSec);
		}
		// Forcibly set this.noStep to false so we can get the first file.
		this.noStep = false;
		step();
		this.noStep = noStep;
	}
	
	/**
	 * Get one buffer's worth of bytes from the stream.
	 * 
	 * @return One buffer's worth of bytes from the stream.
	 * @throws IOException if an error occured with the underlying file.
	 */
	public byte[] getBytes() throws IOException {
		if (bs == null) {
			return new byte[0];
		}
		
		byte[] buffer = new byte[bufferSize];
		
		int totalRead = 0;
		// The call to hasMore() will automatically step forward to the next file as needed.
		while (totalRead < bufferSize && hasMore()) {
			int remaining = bufferSize - totalRead;
			int bytesRead = readThrottled(bs, buffer, totalRead, remaining);
			if (bytesRead <= 0) {
				break;
			}
			if (bytesRead < remaining) {
				// Pad to next block boundary.
				int remainder = bytesRead % blockSize;
				if (remainder != 0) {
					int end = bytesRead + (blockSize - remainder);
					bytesRead += clearBuffer(bytesRead, end, buffer);
				}
				// Get the next file if possible.
				step();
			}
			totalRead += bytesRead;
		}
		
		if (totalRead < bufferSize) {
			clearBuffer(totalRead, bufferSize, buffer);
		}
		
		return buffer;
	}
	
	/**
	 * Check if there's still more data available.
	 * 
	 * @return True if either the current file is open and available or if we can open
	 *         another file.
	 * @throws IOException if an error occured with the underlying file.
	 */
	public boolean hasMore() throws IOException {
		// First, see if we need to advance.
		if (bs == null || bs.available() <= 0) {
			step();
		}
		// Then see if we're available.
		return bs != null && bs.available() > 0;
	}
	
	/**
	 * Get the number of files read by the underlying FileWalker.
	 * 
	 * @return Number of files the underlying FileWalker has accessed.
	 */
	public long getFilesRead() {
		return walker.getFilesAccessed();
	}
	
	/**
	 * A helper function that reads bytes from a BufferedInputStream into a buffer and throttles
	 * the read rate if appropriate.
	 * 
	 * @param bs BufferedInputStream from which to read.
	 * @param buffer Buffer in which to store the read data.
	 * @param start Offset from beginning of buffer in which to start storing data.
	 * @param len Maximum number of bytes to read.
	 * @return The number of bytes read or -1 if the end of stream has been reached.
	 * @throws IOException if the underlying file read threw one.
	 */
	private int readThrottled(BufferedInputStream bs, byte[] buffer, int start, int len) throws IOException {
		long initial = System.currentTimeMillis();
		int bytesRead = bs.read(buffer, start, len);
		long elapsed = System.currentTimeMillis() - initial;
		// This will always be false if there's no limit, since delayMS will be 0.
		if (elapsed < delayMS && bytesRead != -1) {
			try {
				Thread.sleep(delayMS-elapsed);
			} catch (InterruptedException e) {
				// Do nothing, since we don't really care if it's interrupted.
			}
		}
		return bytesRead;
	}
	
	/**
	 * Clear part of the specified buffer.
	 * 
	 * @param start Starting offset from which to clear.
	 * @param end Index after the maximum index to clear (half-closed).
	 * @param buffer Buffer to clear.
	 * @return The number of bytes cleared.
	 */
	private int clearBuffer(int start, int end, byte[] buffer) {
		if (end < start) {
			return 0;
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
		return end - start;
	}
	
	/**
	 * Advance the input stream to point to the next file if it exists. If the underlying FileWalker
	 * doesn't have a next file, the input stream will be null.
	 * 
	 * @throws IOException if the underlying file stream generated an error.
	 */
	private void step() throws IOException {
		if (bs != null) {
			bs.close();
		}
		if (walker.hasNext() && !noStep) {
//         bs = new BufferedInputStream(Files.newInputStream(walker.next()), bufferSize);
         bs = new BufferedInputStream(new FileInputStream(walker.next().toFile()), bufferSize);
      } else {
			bs = null;
		}
	}

	@Override
	public void close() throws IOException {
		if (bs != null) {
			bs.close();
		}
	}
	
	public static void main(String[] args) throws Exception{
System.in.read();
	   FileWalkerStream fws=new FileWalkerStream(
         new FileWalker(Paths.get("../testdata/in"), false), 1000, 100000, CompScan.UNLIMITED, false
      );
      if(fws.hasMore()) fws.getBytes();
      int bytes=0;
      long t0=System.currentTimeMillis();
      while(fws.hasMore()){
         byte[] buf=fws.getBytes();
         bytes+=buf.length;
         if((bytes&0xffff)==0) System.out.println(bytes+" bytes read");
      }
      long dt=System.currentTimeMillis()-t0;
      System.out.println("total: "+bytes+" bytes");
      System.out.println("dt: "+dt);
      System.out.println("TP: "+(bytes*1000./dt)+" bytes/s");
   }
}
