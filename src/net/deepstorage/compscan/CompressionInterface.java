/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan;

import net.deepstorage.compscan.Compressor.BufferLengthException;

/**
 * The CompressionInterface presents the necessary mechanism for a compression algorithm to
 * interface with CompScan. Its only requirement is the compress method.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public interface CompressionInterface {
	/**
	 * Compress the data. This is the only method necessary for a compression algorithm to
	 * interface with CompScan. It should take an array of bytes, one superblock in size,
	 * and return a new, smaller array of compressed bytes.
	 * 
	 * Java interfaces do not have a mechanism for explicitly setting the size of the input
	 * array. In order to keep CompressionInterface as simple as possible, this limitation
	 * is not encapsulated in a wrapper class. Instead, Compressor explicitly checks to
	 * make sure the size of the input buffer is one superblock in size before calling
	 * compress.
	 * 
	 * @param data Data to compress. Should be one superblock in size.
	 * @param blockSize Size of the compression blocks.
	 * @return A smaller array of bytes containing the compressed data.
	 * @throws BufferLengthException if data is not 
	 */
	public byte[] compress(byte[] data, int blockSize) throws BufferLengthException;
}
