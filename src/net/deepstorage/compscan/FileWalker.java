/**
 * CompScan - a tool for estimating the compressibility of a dataset.
 * 
 * Copyright (c) 2016 DeepStorage, LLC (deepstorage.net) and Ramon A. Lovato (ramonalovato.com).
 * 
 * See the file LICENSE for copying permission.
 */
package net.deepstorage.compscan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.stream.Stream;

/**
 * A filesystem scanner for CompScan.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class FileWalker implements AutoCloseable {
	private Path root;
	private CompScan.ScanMode mode;
	private Stream<Path> fileStream;
	private Iterator<Path> iterator;
	private Queue<Path> current;
	
	/**
	 * Create a new FileScanner beginning at root.
	 * @throws IOException if the file stream couldn't be opened.
	 */
	public FileWalker(Path root, CompScan.ScanMode mode) throws IOException {
		this.root = root;
		this.mode = mode;
		
		fileStream = Files.walk(this.root);
		iterator = fileStream.iterator();
		current = new LinkedList<Path>();
	}
	
	/**
	 * Advance to the next file.
	 * 
	 * @return Path to the next file or null if no next file.
	 */
	public Path next() {
		
		return (iterator.hasNext() ? iterator.next() : null);
	}
	
	/**
	 * Check if the iterator has a next element.
	 * 
	 * @return True if the iterator has a next element.
	 */
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public void close() {
		fileStream.close();
		System.out.println("FileScanner closed.");
	}
}
