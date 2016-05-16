/**
 * 
 */
package net.deepstorage.compscan;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import net.deepstorage.compscan.CompScan.Results;

/**
 * @author user1
 *
 */
public class FileScanner {
	
	/**
	 * Scan a VMDK.
	 * 
	 * @param res Results object in which to save the results.
	 * @throws IOException if an IO error occurs.
	 */
	private void scanVMDK(Results res) throws IOException {
		scanFile(pathIn, null, res);
	}
	
	/**
	 * Scan a directory.
	 * 
	 * @param res Results object in which to save the results.
	 * @throws IOException if file access failed.
	 */
	private void scanDirectory(Results res) throws IOException {
		try (FileWalker walker = new FileWalker(pathIn)) {
			// TODO
		}
		
	}
	
	/**
	 * Scan a file.
	 * 
	 * @param p1 Primary file to scan.
	 * @param p2 If scanning p1 reaches the end of the file in the middle of a superblock,
	 *           scanning continues with the beginning of p2 until the superblock is filled.
	 *           If null, the superblock is terminated prematurely.
	 * @param res Results object in which to save the results.
	 * @throws IOException if file access failed.
	 */
	private void scanFile(Path p1, Path p2, Results res) throws IOException {
		BufferedInputStream bs1 = new BufferedInputStream(Files.newInputStream(p1), buffSize);
		BufferedInputStream bs2 = new BufferedInputStream(Files.newInputStream(p2), buffSize);
		
		bs1.close();
		bs2.close();
	}
}
