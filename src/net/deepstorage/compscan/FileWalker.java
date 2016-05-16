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
import java.util.stream.Stream;

/**
 * A filesystem scanner for CompScan.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class FileWalker implements AutoCloseable {
	private Path root;
	private Stream<Path> fileStream;
	private Iterator<Path> iterator;
	private Path previous;
	private Path next;
	
	/**
	 * Create a new FileScanner beginning at root.
	 * 
	 * @param root Path to the root of the datastore to scan.
	 * @throws IOException if the file stream couldn't be opened.
	 */
	public FileWalker(Path root) throws IOException {
		this.root = root;
		fileStream = Files.walk(this.root);
		iterator = fileStream.iterator();
		previous = null;
		next = null;
	}
	
	/**
	 * Advance to the next file.
	 * 
	 * @return Path to the next file or null if no next file.
	 */
	public Path next() {
		previous = next;
		next = (iterator.hasNext() ? iterator.next() : null);
		return next;
	}
	
	/**
	 * Get the previous file.
	 * 
	 * @return Path to the previous file or null if no previous file.
	 */
	public Path previous() {
		return previous;
	}
	
	/**
	 * Check if the iterator has a next element.
	 * 
	 * @return True if the iterator has a next element.
	 */
	public boolean hasNext() {
		return iterator.hasNext();
	}
	
	/**
	 * Check if the walker has a previous element.
	 * 
	 * @return True if the walker has a previous element. 
	 */
	public boolean hasPrevious() {
		return previous != null;
	}
	
	/**
	 * Get the stream iterator currently in use.
	 * 
	 * @return Iterator<Path> currently being used.
	 */
	public Iterator<Path> getIterator() {
		return iterator;
	}
	
	/**
	 * Get the root of the datastore being scanned.
	 * 
	 * @return Root of the datastore being scanned.
	 */
	public Path getRoot() {
		return root;
	}

	@Override
	public void close() {
		fileStream.close();
		System.out.println("FileScanner closed.");
	}
}
