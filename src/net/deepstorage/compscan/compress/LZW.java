/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan.compress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.deepstorage.compscan.CompressionInterface;

/**
 * A simple LZW implementation.
 * 
 * @see https://rosettacode.org/wiki/LZW_compression#Java
 * @version 1.0
 */
public class LZW implements CompressionInterface {
//	// Build the dictionary.
//    int dictSize = 256;
//    Map<String,Integer> dictionary = new HashMap<String,Integer>();
//    for (int i = 0; i < 256; i++)
//        dictionary.put("" + (char)i, i);
//
//    String w = "";
//    List<Integer> result = new ArrayList<Integer>();
//    for (char c : uncompressed.toCharArray()) {
//        String wc = w + c;
//        if (dictionary.containsKey(wc))
//            w = wc;
//        else {
//            result.add(dictionary.get(w));
//            // Add wc to the dictionary.
//            dictionary.put(wc, dictSize++);
//            w = "" + c;
//        }
//    }
//
//    // Output the code for w.
//    if (!w.equals(""))
//        result.add(dictionary.get(w));
//    return result;
	
	@Override
	public byte[] compress(byte[] data, int blockSize) {
		// Build the dictionary.
        int dictSize = 256;
        Map<String,Byte> dictionary = new HashMap<String,Byte>();
        for (int i = 0; i < 256; i++)
            dictionary.put("" + (byte) i, (byte) i);
 
        String w = "";
        List<Byte> result = new ArrayList<Byte>();
        for (byte b : data) {
            String wb = w + b;
            if (dictionary.containsKey(wb))
                w = wb;
            else {
                result.add(dictionary.get(w));
                // Add wc to the dictionary.
                dictionary.put(wb, (byte) dictSize++);
                w = "" + b;
            }
        }
 
        // Output the code for w.
        if (!w.equals(""))
            result.add(dictionary.get(w));
        
        byte[] output = new byte[result.size()];
        for (int i = 0; i < output.length; i++) {
        	output[i] = result.get(i);
        }
        return output;
    }

}
