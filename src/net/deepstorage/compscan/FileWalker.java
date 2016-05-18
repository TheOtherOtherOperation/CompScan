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
	private Stream<Path> fileStream;
	private Iterator<Path> iterator;
	private Queue<Path> pending;
	
	/**
	 * Create a new FileScanner beginning at root.
	 * 
	 * @param root Path to the root of the datastore to scan.
	 * @throws IOException if the file stream couldn't be opened.
	 */
	public FileWalker(Path root) throws IOException {
		this.root = root;
		fileStream = Files.walk(this.root).filter(f -> Files.isRegularFile(f));
		iterator = fileStream.iterator();
		pending = new LinkedList<Path>();
		System.out.format("FileWalker opened to \"%s\".%n%n", root);
	}
	
	/**
	 * Advance to the next file.
	 * 
	 * @return Path to the next file.
	 */
	public Path next() {
		Path p = (pending.size() > 0 ? pending.poll() : iterator.next());
//		System.out.format("Reading file: \"%s\".%n", p.toString());
		return p;
	}
	
	/**
	 * Peek the next file without consuming it completely. The file is removed from the
	 * iterator but added to the pending queue. The next time next() is called, the
	 * head of the pending queue is returned instead of advancing the iterator. The
	 * iterator is only advanced once the queue is empty. This method may be called
	 * multiple times. Each subsequent call will add another item to the queue.
	 * 
	 * @return Path to the next file added to the pending queue.
	 */
	public Path lookAhead() {
		Path next = iterator.next();
		pending.add(next);
		return next;
	}
	
	/**
	 * Check if it's still possible to look ahead by seeing if the iterator has a next
	 * element. The state of the pending queue is not considered.
	 * 
	 * @return True if the iterator has a next element.
	 */
	public boolean hasLookAhead() {
		return iterator.hasNext();
	}
	
	/**
	 * Check if the iterator has a next element.
	 * 
	 * @return True if the pending queue or iterator has a next element.
	 */
	public boolean hasNext() {
		return pending.size() > 0 || iterator.hasNext();
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
		System.out.format("%nFileWalker closed.%n%n");
	}
}
