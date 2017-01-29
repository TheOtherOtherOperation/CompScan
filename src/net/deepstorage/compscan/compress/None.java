/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan.compress;

import net.deepstorage.compscan.CompressionInterface;

/**
 * None is a direct pass-through scheme that returns the input buffer unchanged. Used for testing.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class None implements CompressionInterface {
   public void setOptions(String s){
      //noop
   }
   
   @Override
	public byte[] compress(byte[] data, int blockSize) {
		// TODO Auto-generated method stub
		return data;
	}

}
