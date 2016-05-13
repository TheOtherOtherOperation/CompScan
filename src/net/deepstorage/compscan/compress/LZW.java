/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan.compress;

import net.deepstorage.compscan.Compressor;

/**
 * An LZW Compressor for CompScan.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class LZW extends Compressor {

	public LZW(int buffSize, int blockSize, int superblockSize, String formatString)
			throws IllegalArgumentException {
		super(buffSize, blockSize, superblockSize, formatString);
	}

	@Override
	public byte[] compress(byte[] data) {
		// TODO Auto-generated method stub
		return data;
	}

}
