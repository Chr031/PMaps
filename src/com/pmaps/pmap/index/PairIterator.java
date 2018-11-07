package com.pmaps.pmap.index;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;

import com.pmaps.PMapException;
import com.pmaps.pmap.PMap;
import com.pmaps.pmap.pair.Pair;

/**
 * TODO : Pair iterator should be re-factored to map the structure needed and
 * returned by the method {@link #entrySet()} : Set<Entry<K,V>>.
 * 
 * 
 * @author Bleu
 * 
 * @param <Kp>
 * @param <Vp>
 */
public class PairIterator<K, V> implements Iterator<Pair<K, V>> {

	private final PMap<K, V> pMap;

	private final LinkedList<BTreeNodePosition<K, V>> currentBTreeNodePositionBranch;

	private Enumeration<Pair<K, V>> currentPairEnumeration;
	private Enumeration<Pair<K, V>> nextPairEnumeration;

	private volatile boolean concurrentModification;

	public PairIterator(PMap<K, V> pMap) throws IOException {

		this.pMap = pMap;

		this.pMap.registerOngoingIterator(this);

		long topIndexPosition = pMap.getNodeDriver().getTopIndexPosition();
		BTreeNode<K, V> topNode = new BTreeNode<>(pMap, topIndexPosition);
		currentBTreeNodePositionBranch = new LinkedList<>();
		currentBTreeNodePositionBranch.add(new BTreeNodePosition<K, V>(topNode));

		currentPairEnumeration = getNextPairEnumeration(currentBTreeNodePositionBranch);
		if (currentBTreeNodePositionBranch.size() > 0) {
			currentBTreeNodePositionBranch.getLast().switcher = currentBTreeNodePositionBranch.getLast().switcher.next();
			nextPairEnumeration = getNextPairEnumeration(currentBTreeNodePositionBranch);
		} else
			nextPairEnumeration = null;
	}

	/**
	 * Recursive method. Based on the last element of the given bTreeNodeList
	 * add all the most left
	 * 
	 * @param topNode
	 * @param currentBTreeNodeBranch2
	 * @throws IOException
	 */
	protected Enumeration<Pair<K, V>> getNextPairEnumeration(LinkedList<BTreeNodePosition<K, V>> bTreeNodeList) throws IOException {
		BTreeNodePosition<K, V> bTreeNodePosition = bTreeNodeList.getLast();

		// debug purpose
		// System.out.println(bTreeNodePosition.bTreeNode + " " +
		// bTreeNodePosition.switcher);

		switch (bTreeNodePosition.switcher) {
		case BEFORE:
			if (bTreeNodePosition.bTreeNode.beforeTreePointer != PMap.EOF_POSITION) {
				final BTreeNode<K, V> nextBTreeNode = new BTreeNode<K, V>(pMap, bTreeNodePosition.bTreeNode.beforeTreePointer);
				final BTreeNodePosition<K, V> nextBTreeNodePosition = new BTreeNodePosition<K, V>(nextBTreeNode);
				bTreeNodeList.add(nextBTreeNodePosition);
			} else {
				// not necessary ! --> but clearer more intuitive code.
				// could simply give the hand to the next case !
				bTreeNodePosition.switcher = BTreeNodeSwitcher.HASH1;
			}
			break;

		case HASH1:
			if (bTreeNodePosition.bTreeNode.nextPair1Pointer != PMap.EOF_POSITION)
				return bTreeNodePosition.bTreeNode.getPairEnumeration(bTreeNodePosition.bTreeNode.keyHash1);
			else
				// not necessary ! --> but clearer more intuitive code.
				//
				bTreeNodePosition.switcher = BTreeNodeSwitcher.CENTER;
			break;

		case CENTER:
			if (bTreeNodePosition.bTreeNode.centerTreePointer != PMap.EOF_POSITION) {
				final BTreeNode<K, V> nextBTreeNode = new BTreeNode<K, V>(pMap, bTreeNodePosition.bTreeNode.centerTreePointer);
				final BTreeNodePosition<K, V> nextBTreeNodePosition = new BTreeNodePosition<K, V>(nextBTreeNode);
				bTreeNodeList.add(nextBTreeNodePosition);
			} else {
				// not necessary ! --> but clearer more intuitive code.
				//
				bTreeNodePosition.switcher = BTreeNodeSwitcher.HASH2;
			}
			break;

		case HASH2:
			if (bTreeNodePosition.bTreeNode.keyHash1 != bTreeNodePosition.bTreeNode.keyHash2
					&& bTreeNodePosition.bTreeNode.nextPair2Pointer != PMap.EOF_POSITION)
				return bTreeNodePosition.bTreeNode.getPairEnumeration(bTreeNodePosition.bTreeNode.keyHash2);
			else {
				// not necessary ! --> but clearer more intuitive code.
				//
				bTreeNodePosition.switcher = BTreeNodeSwitcher.AFTER;
			}
			break;

		case AFTER:
			if (bTreeNodePosition.bTreeNode.afterTreePointer != PMap.EOF_POSITION) {
				final BTreeNode<K, V> nextBTreeNode = new BTreeNode<K, V>(pMap, bTreeNodePosition.bTreeNode.afterTreePointer);
				final BTreeNodePosition<K, V> nextBTreeNodePosition = new BTreeNodePosition<K, V>(nextBTreeNode);
				bTreeNodeList.add(nextBTreeNodePosition);
			} else {
				BTreeNodePosition<K, V> previousBTreeNodePosition;
				do {
					bTreeNodeList.removeLast();

					if (bTreeNodeList.size() == 0)
						return null;
					previousBTreeNodePosition = bTreeNodeList.getLast();
					previousBTreeNodePosition.switcher = previousBTreeNodePosition.switcher.next();

				} while (previousBTreeNodePosition.switcher == BTreeNodeSwitcher.BEFORE && bTreeNodeList.size() > 0);
			}
			break;
		}

		return getNextPairEnumeration(bTreeNodeList);

	}

	public void onConncurrentModification() {
		concurrentModification = true;
	}

	protected boolean isInConcurrentState() {
		if (concurrentModification)
			throw new ConcurrentModificationException();
		return concurrentModification;
	}

	@Override
	public boolean hasNext() {

		return (currentPairEnumeration != null && currentPairEnumeration.hasMoreElements())
				|| (nextPairEnumeration != null && nextPairEnumeration.hasMoreElements());
	}

	@Override
	public Pair<K, V> next() {

		if (currentPairEnumeration == null)
			throw new NoSuchElementException();

		try {
			Lock readLock = pMap.getReadLock();
			readLock.lockInterruptibly();

			try {

				isInConcurrentState();

				try {

					return currentPairEnumeration.nextElement();
				} catch (NoSuchElementException nsee) {
					if (nextPairEnumeration != null) {
						currentPairEnumeration = nextPairEnumeration;
						currentBTreeNodePositionBranch.getLast().switcher = currentBTreeNodePositionBranch.getLast().switcher.next();
						try {
							nextPairEnumeration = getNextPairEnumeration(currentBTreeNodePositionBranch);
						} catch (IOException e) {
							throw new NoSuchElementException(e.getMessage());
						}
						return next();
					}
				}
			} finally {
				readLock.unlock();
			}
		} catch (InterruptedException e) {
			throw new PMapException("Interruption while running and iterration", e);
		}

		throw new NoSuchElementException();

	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not yet implemented");

	}

}
