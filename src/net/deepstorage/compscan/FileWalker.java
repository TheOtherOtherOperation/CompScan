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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.stream.Stream;

import net.deepstorage.compscan.CompScan.ScanMode;

/**
 * A filesystem walker for CompScan.
 * 
 * @author Ramon A. Lovato
 * @version 1.0
 */
public class FileWalker implements AutoCloseable {
	private Path root;
	private Stream<Path> fileStream;
	private Iterator<Path> iterator;
	private Queue<Path> pending;
	private long filesAccessed;
	private boolean verbose;
	
	/**
	 * Create a new FileScanner beginning at root.
	 * 
	 * @param root Path to the root of the datastore to scan.
	 * @param verbose Whether or not to enable verbose console logging.
	 * @throws IOException if the file stream couldn't be opened.
	 */
	public FileWalker(Path root, boolean verbose) throws IOException {
		this(root, ScanMode.NORMAL, verbose);
	}
	
	/**
	 * Create a new FileScanner beginning at root with file extension white-listing.
	 * 
	 * @param root Path to the root of the datastore to scan.
	 * @param scanMode The ScanMode to use. If NORMAL, the resulting file stream will
	 *                 contain all regular files. If VMDK, the file stream will contain
	 *                 only those files whose extensions are in CompScan.VALID_EXTENSIONS.
	 * @param verbose Whether or not to enable verbose console logging.
	 * @throws IOException if the file stream couldn't be opened.
	 */
	public FileWalker(Path root, ScanMode scanMode, boolean verbose) throws IOException {
		this.root = root;
		this.verbose = verbose;
		
		if (scanMode == ScanMode.VMDK) {
			fileStream = Files.walk(this.root).filter(f -> isVMDK(f));
		} else {
			fileStream = Files.walk(this.root).filter(f -> Files.isRegularFile(f));
		}
		
		iterator = fileStream.iterator();
		pending = new LinkedList<Path>();
		filesAccessed = 0;
	}
	
	/**
	 * Advance to the next file.
	 * 
	 * @return Path to the next file.
	 */
	public Path next() {
		Path p = (pending.size() > 0 ? pending.poll() : iterator.next());
		if (verbose && p != null) {
			System.out.println("Opening file: \"" + p.toString() + "\"");
		}
		filesAccessed++;
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
		if (verbose && next != null) {
			System.out.println("Looking ahead to file: \"" + next.toString() + "\"");
		}
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
		return iterator.hasNext() || pending.size() > 0;
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
	
	/**
	 * Get the number of files accessed.
	 * 
	 * @return The number of files accessed via next().
	 */
	public long getFilesAccessed() {
		return filesAccessed;
	}

	@Override
	public void close() {
		fileStream.close();
	}
	
	/**
	 * Check if path is a valid virtual disk file.
	 * 
	 * @param path Path to verify.
	 * @return True if path is a valid virtual disk file.
	 */
	private static Boolean isVMDK(Path path) {
		if (path == null) {
			return false;
		} else {
			String[] partials = path.getFileName().toString().split("\\.(?=\\w+$)");
			if (partials.length < 2) {
				return false;
			}
			// Short-circuits.
			return (Files.isRegularFile(path)
					&& partials.length == 2
					&& Arrays.asList(CompScan.VALID_EXTENSIONS).contains(partials[1].toLowerCase()));
		}
	}
}
