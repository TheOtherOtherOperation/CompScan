/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The abstract Compressor class defines the procedures needed for a compression scheme to be used with CompScan.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class Compressor {
	private CompressionInterface compressionInterface;
	private byte[] buffer;
	private final int readBuffSize;
	private final int blockSize;
	private final int superblockSize;
	private final String formatString;
	private long bytesRead;
	private long blocksRead;
	private long superblocksRead;
	private long compressedBytes;
	private long compressedBlocks;
	private long actualBytes;
	
	/**
	 * Instantiate a new Compressor.
	 * 
	 * @param readBuffSize Size of the input buffer in bytes.
	 * @param blockSize The block size of the compression scheme in bytes.
	 * @param superblockSize The superblock size of the compression scheme in bytes.
	 * @param formatString The name of the compression scheme.
	 * @throws IllegalArgumentException if buffSize, blockSize, or superblockSize are nonpositive, or if formatString is empty or null.
	 */
	protected Compressor(int readBuffSize, int blockSize, int superblockSize, String formatString) throws IllegalArgumentException {
		if (readBuffSize < 1) {
			throw new IllegalArgumentException("Buffer size must be a positive integer.");
		}
		this.readBuffSize = readBuffSize;
		if (blockSize < 1 || blockSize >= readBuffSize) {
			throw new IllegalArgumentException(String.format(
					"Block size (%d) must be a positive integer greater than or equal to read buffer size (%d).",
					blockSize, readBuffSize));
		}
		this.blockSize = blockSize;
		if (superblockSize < 1 || superblockSize % blockSize != 0) {
			throw new IllegalArgumentException(String.format(
					"Superblock size (%d) must be a positive integer multiple of block size (%d).",
					superblockSize, blockSize));
		}
		this.superblockSize = superblockSize;
		if (formatString == null || formatString.length() == 0) {
			throw new IllegalArgumentException("Format string cannot be null or empty string.");
		}
		this.formatString = formatString;
		try {
			compressionInterface = getCompressionInterface(formatString);
			System.out.println(String.format("Using compression interface \"%s\".%n", formatString));
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(
					String.format(
							"Unable to locate Compressor for compression format \"%s\".", formatString));
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		bytesRead = 0L;
		blocksRead = 0L;
		superblocksRead = 0L;
		compressedBytes = 0L;
		compressedBlocks = 0L;
		actualBytes = 0L;
		buffer = new byte[superblockSize];
		clearBuffer();
	}
	
	/**
	 * Clear the internal data buffer.
	 */
	private void clearBuffer() {
		for (int i = 0; i < buffer.length; i++) {
			buffer[i] = 0x0;
		}
	}
	
	/**
	 * Get the CompressionInterface for the specified format string.
	 * 
	 * @param formatString Name of the compression scheme to retrieve.
	 * @return Method "compress(byte[] data)" associated with the CompressionInterface for formatString.
	 * @throws IllegalArgumentException if the Compressor for the format string does not exist.
	 */
	private CompressionInterface getCompressionInterface(String formatString) throws Exception {	
		String compressName = String.join(".", getClass().getPackage().getName(), CompScan.COMPRESSION_SUBPACKAGE, formatString);
		
		Class<?> compression = Class.forName(compressName);
		Set<Class<?>> interfaces = new HashSet<>(
				Arrays.asList(
						compression.getInterfaces()));
		if (!interfaces.contains(CompressionInterface.class)) {
			throw new Exception(
					String.format(
							"Class \"%1$s\" found for format string \"%2$s\" but is not a valid Compressor.",
							compression.getClass().getName(), formatString));
		}
		return (CompressionInterface) compression.newInstance();
	}
	
	/**
	 * Getter for buffer size.
	 * 
	 * @return Size of the internal buffer in bytes.
	 */
	public int getReadBufferSize() {
		return readBuffSize;
	}
	
	/**
	 * Getter for the data buffer size.
	 * 
	 * @return Size of the data buffer in bytes.
	 */
	public int getDataBufferSize() {
		return buffer.length;
	}
	
	/**
	 * Getter for block size.
	 * 
	 * @return Size of the block size in bytes.
	 */
	public int getBlockSize() {
		return blockSize;
	}
	
	/**
	 * Getter for superblock size.
	 * 
	 * @return Size of the superblock size in bytes.
	 */
	public int getSuperblockSize() {
		return superblockSize;
	}
	
	/**
	 * Getter for format string.
	 * 
	 * @return Name of the compression scheme.
	 */
	public String getFormatString() {
		return formatString;
	}
	
	/**
	 * Feed a data buffer into the Compressor.
	 * 
	 * @param data Data buffer containing exactly one superblock of data to feed into the Compressor.
	 * @return CompressionInfo with the results of the compression.
	 * @throws BufferLengthException if data.length > buffSize.
	 */
	public CompressionInfo feedData(byte[] data) throws BufferLengthException {
		if (data.length != buffer.length) {
			throw new BufferLengthException(
					String.format(
							"Compressor.feedData requires exactly one superblock of data: $1%d bytes given, $2%d bytes expected.",
							data.length, buffer.length));
		}
		
		// We want the input data to be exactly one superblock in size. 
		byte[] compressed = compressionInterface.compress(data, blockSize);

		CompressionInfo ci = new CompressionInfo(data.length, compressed.length);
		bytesRead += ci.bytesRead;
		blocksRead += ci.blocksRead;
		superblocksRead += 1L;
		compressedBytes += ci.compressedBytes;
		compressedBlocks += ci.compressedBlocks;
		actualBytes += ci.actualBytes;
		return ci;
	}
	
	/**
	 * Get the total compression info for all data passed to feedData up until now.
	 * 
	 * @return CompressionInfo containing the compression info for all data fed up until now.
	 */
	public CompressionInfo getCompressionInfo() {
		return new CompressionInfo(bytesRead, blocksRead, superblocksRead, compressedBytes,
				                   compressedBlocks, actualBytes);
	}
	
	/**
	 * Nested data class for encapsulating compression info.
	 */
	public class CompressionInfo {
		public final long bytesRead;
		public final long blocksRead;
		public final long superblocksRead;
		public final long compressedBytes;
		public final long compressedBlocks;
		public final long actualBytes;
		
		/**
		 * Instantiate a new CompressionInfo from raw values.
		 * 
		 * @param bytesRead Initial size of the data.
		 * @param blocksRead Total blocks read.
		 * @param superblocksRead Total superblocks read.
		 * @param compressedBytes Number of bytes after compression.
		 * @param compressedBlocks Number of blocks needed to hold the comrpessed data, rounded up.
		 * @param actualBytes Actual number of bytes needed to store compressedBlocks blocks.
		 */
		private CompressionInfo(long bytesRead, long blocksRead, long superblocksRead,
				                long compressedBytes, long compressedBlocks, long actualBytes) {
			this.bytesRead = bytesRead;
			this.blocksRead = blocksRead;
			this.superblocksRead = superblocksRead;
			this.compressedBytes = compressedBytes;
			this.compressedBlocks = compressedBlocks;
			this.actualBytes = actualBytes;
		}
		
		/**
		 * Instantiate a new CompressionInfo from the bytes read and size of the compressed data.
		 * 
		 * @param bytesRead Initial size of the data.
		 * @param compressedBytes Size of the compressed data.
		 */
		private CompressionInfo(long bytesRead, long compressedBytes) throws BufferLengthException {
			this.bytesRead = bytesRead;
			this.compressedBytes = compressedBytes;
			
			blocksRead = bytesRead / blockSize;
			superblocksRead = bytesRead / superblockSize;
			
			compressedBlocks = (compressedBytes % blockSize == 0 ?
					compressedBytes / blockSize : compressedBytes / blockSize + 1);
			actualBytes = compressedBlocks * blockSize;
		}
	}
	
	/**
	 * Nested exception for invalid buffer size.
	 */
	public static class BufferLengthException extends Exception {
		public BufferLengthException(String message) {
			super(message);
		}
	}
}
