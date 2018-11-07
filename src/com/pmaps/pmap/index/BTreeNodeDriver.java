package com.pmaps.pmap.index;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

import com.pmaps.PMapException;
import com.pmaps.pmap.PMap;
import com.pmaps.pmap.filedriver.FileDriver;

/**
 * This class manages all the read access to the underlying file using the file
 * driver.
 * 
 * 
 * @author Bleu
 * 
 * @param <Kp>
 * @param <Vp>
 */

public class BTreeNodeDriver<K, V> {

	private final PMap<K, V> pMap;
	private final FileDriver indexFileDriver;

	public BTreeNodeDriver(PMap<K, V> pMap, FileDriver indexFileDriver) {
		this.pMap = pMap;
		this.indexFileDriver = indexFileDriver;

	}

	protected FileDriver getIndexFileDriver() {
		return indexFileDriver;
	}

	public BTreeNode<K, V> goToIndex(int hashCode) {

		LinkedList<Long> positions;
		try {
			positions = findEntryPositions(hashCode);
			long lastIndexPosition = positions.getLast();
			if (lastIndexPosition != PMap.EOF_POSITION) {
				BTreeNode<K, V> index = new BTreeNode<K, V>(pMap, lastIndexPosition);
				return index;
			}
		} catch (IOException e) {
			throw new PMapException("Index lookup error.", e);
		}
		return null;

	}

	/**
	 * <ul>
	 * Returns the list of all the {@link BTreeNode} positions that lead to the
	 * give hashKey. Starts from the beginning of the file and also from the
	 * root {@link BTreeNode} and goes over all the descending Tree.
	 * </ul>
	 * 
	 * @param hashKey
	 * @return
	 * @throws IOException
	 */
	public LinkedList<Long> findEntryPositions(int hashKey) throws IOException {
		long p = getTopIndexPosition();
		LinkedList<Long> nodePosition = new LinkedList<Long>();

		if (p == PMap.EOF_POSITION) {
			nodePosition.add(p);
			return nodePosition;
		}

		long next;
		try {
			while (true) {
				nodePosition.add(p);
				next = findNextEntryPosition(p, hashKey);
				if (next == p)
					return nodePosition;
				if (next == PMap.EOF_POSITION) {
					nodePosition.add(next);
					return nodePosition;
				}
				p = next;

			}
		} finally {
			// System.out.print(nodePosition.size() + "-|");
		}
	}

	/**
	 * XXX This method duplicates the {@link BTreeNode} read constructor but
	 * optimizes the read in order not to perform all read operations, but only
	 * the one that are necessary. This read has to be completely compatible
	 * with the read constructor present in the {@link BTreeNode} class. See
	 * {@link BTreeNode#BTreeNode(RandomAccessFile, long)}
	 * 
	 * 
	 * @param position
	 * @param hashKey
	 * @return Return the position of the next node where hashKey could be found
	 *         or EOF if the node with hashKey does not exist. If position is
	 *         return the node has been found.
	 * @throws IOException
	 */
	protected long findNextEntryPosition(long position, int hashKey) throws IOException {
		synchronized (indexFileDriver.getAccessLock()) {
			getIndexFileDriver().seek(position);
			try {
				long beforePointer = getIndexFileDriver().getLong();
				int hashKey1 = getIndexFileDriver().readInt();
				if (hashKey1 == hashKey)
					return position;
				else if (hashKey < hashKey1)
					return beforePointer;

				getIndexFileDriver().getLong(); // skip nextPair1Pointer
				long centerPointer = getIndexFileDriver().getLong();
				int hashKey2 = getIndexFileDriver().readInt();

				if (hashKey < hashKey2 || hashKey2 == hashKey1)
					return centerPointer;
				else if (hashKey2 == hashKey)
					return position;

				getIndexFileDriver().getLong(); // skip nextPair2Pointer
				long afterPointer = getIndexFileDriver().getLong();

				/*
				 * at this point we should have hashKey > hashKey2 and we can
				 * return afterPointer.
				 */
				return afterPointer;

			} catch (EOFException eofe) {
				// TODO should raise the error
				eofe.printStackTrace();
				System.err.println("Bad file format ! File is corrupted at " + position);
				return PMap.EOF_POSITION;
			}
		}
	}

	/**
	 * Look at the {@link PMap#initFile}
	 * 
	 * @return
	 * @throws IOException
	 */
	public long getTopIndexPosition() throws IOException {
		synchronized (indexFileDriver.getAccessLock()) {
			getIndexFileDriver().seek(PMap.POSITION_TOP_NODE);
			long p = getIndexFileDriver().getLong();
			return p;
		}
	}

	/**
	 * <p>
	 * Registers a new key entry in the b-tree and returns the BTreeNode where
	 * the new entry lies.
	 * </p>
	 * <p>
	 * This method implements the most important part of the B-Tree algorithm :
	 * The node insertion.
	 * 
	 * </p>
	 * 
	 * @param treePathPositions
	 * @param keyHash
	 * @return
	 * @throws IOException
	 */
	public BTreeNode<K, V> registerNewEntry(LinkedList<Long> treePathPositions, int keyHash, long pairPointer, BTreeNode<K, V> previousLeft,
			BTreeNode<K, V> previousRight) throws IOException {

		// if no node are registered :
		if (treePathPositions.size() == 0) {
			// create a new node that will be the root node :
			BTreeNode<K, V> current = new BTreeNode<>(pMap, keyHash, pollFreeNodePosition());

			current.write(getIndexFileDriver());
			// we register the root node in the file !
			setTopNodePosition(current.position);

			return current;

		}

		BTreeNode<K, V> current = new BTreeNode<>(pMap, treePathPositions.pollLast());

		if (current.keyHash1 == current.keyHash2) {
			/*
			 * place is free, we can insert this new keyHash in the current node
			 * !
			 */

			if (keyHash > current.keyHash1) {

				current.keyHash2 = keyHash;
				current.centerTreePointer = previousLeft == null ? PMap.EOF_POSITION : previousLeft.position;
				current.afterTreePointer = previousRight == null ? PMap.EOF_POSITION : previousRight.position;
				current.nextPair2Pointer = pairPointer;

			} else {

				current.keyHash1 = keyHash;
				current.beforeTreePointer = previousLeft == null ? PMap.EOF_POSITION : previousLeft.position;
				// Transfers the potential pointer and values !
				current.afterTreePointer = current.centerTreePointer;
				current.nextPair2Pointer = current.nextPair1Pointer;
				current.nextPair1Pointer = pairPointer;
				// end with the last pointer !
				current.centerTreePointer = previousRight == null ? PMap.EOF_POSITION : previousRight.position;

			}
			// lets save current
			current.write(getIndexFileDriver());
			return current;
		} else {

			/*
			 * place is not free : we have to split the node current into two
			 * nodes !
			 */
			BTreeNode<K, V> currentLeft, currentRight;
			synchronized (indexFileDriver.getAccessLock()) {
				final long currentLeftPosition = this.pollFreeNodePosition();
				final long currentRightPosition = this.pollFreeNodePosition();
				currentLeft = new BTreeNode<>(pMap, 0, currentLeftPosition);
				currentRight = new BTreeNode<>(pMap, 0, currentRightPosition);

			}
			// 1- find the hash to forward, the left hash and the right one
			// !

			int hashToForward;
			long pairPointerToForward;
			BTreeNode<K, V> resultNode;

			if (current.keyHash1 > keyHash) {
				hashToForward = current.keyHash1;
				pairPointerToForward = current.nextPair1Pointer;

				currentLeft.keyHash1 = currentLeft.keyHash2 = keyHash;
				currentLeft.beforeTreePointer = previousLeft == null ? PMap.EOF_POSITION : previousLeft.position;
				currentLeft.centerTreePointer = previousRight == null ? PMap.EOF_POSITION : previousRight.position;
				currentLeft.nextPair1Pointer = pairPointer;

				currentRight.keyHash1 = currentRight.keyHash2 = current.keyHash2;
				currentRight.beforeTreePointer = current.centerTreePointer;
				currentRight.centerTreePointer = current.afterTreePointer;
				currentRight.nextPair1Pointer = current.nextPair2Pointer;

				// the node of the value
				resultNode = currentLeft;

			} else if (current.keyHash2 < keyHash) {
				hashToForward = current.keyHash2;
				pairPointerToForward = current.nextPair2Pointer;

				currentRight.keyHash1 = currentRight.keyHash2 = keyHash;
				currentRight.beforeTreePointer = previousLeft == null ? PMap.EOF_POSITION : previousLeft.position;
				currentRight.centerTreePointer = previousRight == null ? PMap.EOF_POSITION : previousRight.position;
				currentRight.nextPair1Pointer = pairPointer;

				currentLeft.keyHash1 = currentLeft.keyHash2 = current.keyHash1;
				currentLeft.beforeTreePointer = current.beforeTreePointer;
				currentLeft.centerTreePointer = current.centerTreePointer;
				currentLeft.nextPair1Pointer = current.nextPair1Pointer;

				// the node of the value
				resultNode = currentRight;

			} else {
				// middle position
				hashToForward = keyHash;
				pairPointerToForward = pairPointer;

				currentLeft.keyHash1 = currentLeft.keyHash2 = current.keyHash1;
				currentLeft.beforeTreePointer = current.beforeTreePointer;
				currentLeft.centerTreePointer = previousLeft == null ? PMap.EOF_POSITION : previousLeft.position;
				currentLeft.nextPair1Pointer = current.nextPair1Pointer;

				currentRight.keyHash1 = currentRight.keyHash2 = current.keyHash2;
				currentRight.beforeTreePointer = previousRight == null ? PMap.EOF_POSITION : previousRight.position;
				currentRight.centerTreePointer = current.afterTreePointer;
				currentRight.nextPair1Pointer = current.nextPair2Pointer;

				resultNode = null;// /

			}

			// 2 - save the new nodes
			currentLeft.write(getIndexFileDriver());
			currentRight.write(getIndexFileDriver());
			// set the previous node position as a free node position :
			registerFreeNodePosition(current.position);

			// 3 - forward the process to the upper node !
			if (treePathPositions.size() > 0) {
				// call this method if treePathPositions is not empty ...
				BTreeNode<K, V> upperResultNode = registerNewEntry(treePathPositions, hashToForward, pairPointerToForward, currentLeft, currentRight);

				return resultNode == null ? upperResultNode : resultNode;
			} else {
				// in this case current is the root node : let's create a
				// new root
				synchronized (indexFileDriver.getAccessLock()) {
					BTreeNode<K, V> rootNode = new BTreeNode<>(pMap, hashToForward, getIndexFileDriver().length());
					// reserve the space !
					getIndexFileDriver().setLength(getIndexFileDriver().length() + BTreeNode.BYTE_SIZE);

					rootNode.beforeTreePointer = currentLeft.position;
					rootNode.centerTreePointer = currentRight.position;
					rootNode.nextPair1Pointer = pairPointerToForward;
					rootNode.write(getIndexFileDriver());
					// we register the root node in the file !
					setTopNodePosition(rootNode.position);

					return resultNode == null ? rootNode : resultNode;
				}
			}

		}
	}

	/***
	 * Retrieves the first free node position and reserves it for a new node.
	 * 
	 * @return
	 * @throws IOException
	 */
	protected long pollFreeNodePosition() throws IOException {
		synchronized (indexFileDriver.getAccessLock()) {
			long freeNodePosition = indexFileDriver.getLong(PMap.POSITION_FREE_NODE);
			if (freeNodePosition == PMap.EOF_POSITION) {
				freeNodePosition = indexFileDriver.length();
				// reserve the space for the btree node :
				indexFileDriver.setLength(freeNodePosition + BTreeNode.BYTE_SIZE);
			} else {
				/*
				 * freeNodePosition is inside the file --> update the new
				 * position ( registered as the first long (8 bytes) of this
				 * node )
				 */
				final long newFreePositon = indexFileDriver.getLong(freeNodePosition);
				indexFileDriver.setLong(PMap.POSITION_FREE_NODE, newFreePositon);
			}
			return freeNodePosition;
		}

	}

	/**
	 * Register a new free node position : When a node is invalidated, this
	 * method should be called so that the space in the file could be reused.
	 * See also {@link #pollFreeNodePosition()}
	 * 
	 * Once called the node data is changed.
	 * 
	 * @param position
	 * @throws IOException
	 */
	protected void registerFreeNodePosition(long position) throws IOException {
		synchronized (indexFileDriver.getAccessLock()) {
			long oldFreeNodePosition = indexFileDriver.getLong(PMap.POSITION_FREE_NODE);
			indexFileDriver.setLong(PMap.POSITION_FREE_NODE, position);
			indexFileDriver.setLong(position, oldFreeNodePosition);

		}
	}

	/**
	 * Removes a hash entry from the Btree structure.
	 * 
	 * This is the second main method of this set of classes ....
	 * 
	 * @param treePathPositions
	 * @param hashCode
	 * @throws IOException
	 */
	public void removeHashEntry(LinkedList<Long> treePathPositions, int hashCode) throws IOException {
		BTreeNode<K, V> current = new BTreeNode<>(pMap, treePathPositions.pollLast());

		// 1 - Look for the upper left node.
		LinkedList<BTreeNode<K, V>> nodeList = new LinkedList<BTreeNode<K, V>>();
		long upperLeftPosition = (hashCode == current.keyHash1 ? current.beforeTreePointer : current.centerTreePointer);

		BTreeNode<K, V> upperLeft = null;
		if (upperLeftPosition != PMap.EOF_POSITION) {
			upperLeft = new BTreeNode<K, V>(pMap, upperLeftPosition);
			nodeList.add(upperLeft);
			retrievePathToTheUpperLeftIndex(nodeList);
		}

		// 2 - upper left is identified.
		// we have to replace the values ...
		upperLeft = nodeList.size() > 0 ? nodeList.pollLast() : null;

		if (upperLeft == null) {
			// value is just removed :
			// 2.1 - node has only one value --> must be removed
			if (current.keyHash1 == current.keyHash2) {

				// 2.1 - node has only one value --> must be removed

				if (treePathPositions.size() == 0) {
					/*
					 * 2.1.1 - root Node .
					 * 
					 * we register the new root node position as been the next
					 * center node of the current one:
					 */
					setTopNodePosition(current.centerTreePointer);
				} else {
					/*
					 * 2.1.2 - not root node.
					 * 
					 * The previous node of current must be updated in order to
					 * suppress its link to current :
					 */
					BTreeNode<K, V> previous = new BTreeNode<K, V>(pMap, treePathPositions.getLast());
					long pointerToForward = current.centerTreePointer;
					if (hashCode < previous.keyHash1)
						previous.beforeTreePointer = pointerToForward;
					else if (hashCode > previous.keyHash1 && (previous.keyHash1 == previous.keyHash2 || previous.keyHash2 > hashCode)) {
						previous.centerTreePointer = pointerToForward;
					} else if (previous.keyHash2 < hashCode) {
						previous.afterTreePointer = pointerToForward;
					} else {
						// should never be thrown!
						throw new PMapException("bad index format");
					}
					previous.write(getIndexFileDriver());
				}
			} else {
				// 2.2 node has two distinct values
				// --> should become a single value node
				eraseByShiftingHash(current, hashCode);
				current.write(getIndexFileDriver());
			}
		}

		else {
			// 2.3 upperleft exists --> hash value has to be replaced with
			// upper left value
			int upperLeftKey = upperLeft.keyHash1 < upperLeft.keyHash2 ? upperLeft.keyHash2 : upperLeft.keyHash1;
			long upperLeftPairPointer = upperLeft.keyHash1 < upperLeft.keyHash2 ? upperLeft.nextPair2Pointer : upperLeft.nextPair1Pointer;

			if (current.keyHash1 == hashCode) {
				// we switch the values
				current.keyHash1 = upperLeftKey;
				current.nextPair1Pointer = upperLeftPairPointer;
				if (current.keyHash2 == hashCode) {
					// case of a single node
					current.keyHash2 = upperLeftKey;

				}

			} else if (current.keyHash2 == hashCode) {
				current.keyHash2 = upperLeftKey;
				current.nextPair2Pointer = upperLeftPairPointer;
			} else {
				// should never be thrown!
				throw new PMapException("bad index format");
			}
			current.write(getIndexFileDriver());

			// 2.3.1 upperleft is a single value node --> must be removed
			if (upperLeft.keyHash1 == upperLeft.keyHash2) {
				// let's get the previous node of upperLeft
				if (nodeList.size() > 0) {
					BTreeNode<K, V> previous = nodeList.getLast();
					if (previous.keyHash1 == previous.keyHash2)
						previous.centerTreePointer = upperLeft.beforeTreePointer;
					else
						previous.afterTreePointer = upperLeft.beforeTreePointer;
					previous.write(getIndexFileDriver());
				} else {
					// previous node of upperLeft is current :
					if (current.keyHash1 == upperLeftKey) {
						current.beforeTreePointer = upperLeft.beforeTreePointer;
					} else if (current.keyHash2 == upperLeftKey) {
						current.centerTreePointer = upperLeft.beforeTreePointer;
					}
					current.write(getIndexFileDriver());

				}
			} else {
				// 2.3.2 upperLeft has two values --> only the replaced
				// value must be removed.
				upperLeft.keyHash2 = upperLeft.keyHash1;
				upperLeft.afterTreePointer = PMap.EOF_POSITION;
				upperLeft.nextPair2Pointer = PMap.EOF_POSITION;

				upperLeft.write(getIndexFileDriver());
			}
		}

		// index is yet updated but not really balanced ! +-
	}

	protected void retrievePathToTheUpperLeftIndex(LinkedList<BTreeNode<K, V>> nodeList) throws IOException {
		BTreeNode<K, V> current = nodeList.getLast();
		long upperLeftPosition = (current.keyHash1 == current.keyHash2 ? current.centerTreePointer : current.afterTreePointer);

		if (upperLeftPosition != PMap.EOF_POSITION) {

			nodeList.add(new BTreeNode<K, V>(pMap, upperLeftPosition));
			retrievePathToTheUpperLeftIndex(nodeList);
		}
	}

	protected void eraseByShiftingHash(BTreeNode<K, V> node, int hashCode) {
		if (node.keyHash1 == hashCode) {
			// we shift the values
			node.keyHash1 = node.keyHash2;
			node.beforeTreePointer = node.centerTreePointer;
			node.centerTreePointer = node.afterTreePointer;
			node.nextPair1Pointer = node.nextPair2Pointer;
			node.afterTreePointer = PMap.EOF_POSITION;
			node.nextPair2Pointer = PMap.EOF_POSITION;
		} else if (node.keyHash2 == hashCode) {
			node.keyHash2 = node.keyHash1;
			node.centerTreePointer = node.afterTreePointer;
			node.afterTreePointer = PMap.EOF_POSITION;
			node.nextPair2Pointer = PMap.EOF_POSITION;
		} else {
			// should never be thrown!
			throw new PMapException("bad index format");
		}
	}

	protected void setTopNodePosition(long p) throws IOException {
		synchronized (indexFileDriver.getAccessLock()) {
			indexFileDriver.seek(PMap.POSITION_TOP_NODE);
			indexFileDriver.writeLong(p);
		}

	}

}