package com.pmaps.pmap.pair;

import java.io.IOException;
import java.util.Enumeration;

import com.pmaps.PMapException;
import com.pmaps.pmap.PMap;
import com.pmaps.pmap.filedriver.FileDriver;
import com.pmaps.pmap.index.BTreeNode;

public class PairDriver<K, V> {

	public static class PairTuple<Kp, Vp> {

		public final Pair<Kp, Vp> pair;
		public final boolean pairLessNode;

		public PairTuple(boolean pairLessNode, Pair<Kp, Vp> pair) {
			super();
			this.pairLessNode = pairLessNode;
			this.pair = pair;
		}

	}

	
	private final PMap<K, V> pMap;
	private final FileDriver pairFileDriver;
	private final PairFactory pairFactory;

	public PairDriver(PMap<K, V> pMap, final FileDriver pairFileDriver, PairFactory pairFactory) {
		this.pMap = pMap;
		this.pairFileDriver = pairFileDriver;
		this.pairFactory = pairFactory;

	}

	public FileDriver getPairFileDriver() {
		return pairFileDriver;
	}

	public PairFactory getPairFactory() {
		return pairFactory;
	}

	/**
	 * XXX <b>Must to be called in a synchronized way !!!</b>
	 * 
	 * @param current
	 * @param hashCode
	 * @param key
	 * @param value
	 * @return the old {@link Pair} instance if any or null.
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public Pair<K, V> addPair(BTreeNode<K, V> current, int hashCode, K key, V value) throws ClassNotFoundException, IOException {
		Pair<K, V> previousPair = null;
		Pair<K, V> targetPair = null;

		Enumeration<Pair<K, V>> pairEnum = current.getPairEnumeration(hashCode);
		while (pairEnum.hasMoreElements()) {
			Pair<K, V> pair = pairEnum.nextElement();
			if (pair.getKey().equals(key)) {
				targetPair = pair;
				break;
			}
			previousPair = pair;
		}

		Pair<K, V> newPair = pairFactory.newPairForWriting(pMap, getPairFileDriver().length(), key, value);
		//new Pair<>(pMap, getPairFileDriver().length(), key, value);
		poolFreePairPosition(newPair);
		if (previousPair != null) {
			previousPair.setNextPairPointer(newPair.getPairPointer());
			if (targetPair != null) {
				newPair.setNextPairPointer(targetPair.getNextPairPointer());
			}
		} else {
			current.setNextPairPointer(hashCode, newPair.getPairPointer());
		}

		newPair.write(getPairFileDriver());
		if (previousPair != null) {
			previousPair.write(getPairFileDriver());
		} else {
			current.write(pMap.getIndexFileDriver());
		}

		return targetPair;
	}

	public PairTuple<K, V> removePair(BTreeNode<K, V> current, int hashCode, Object key) throws ClassNotFoundException, IOException {

		Enumeration<Pair<K, V>> pairEnum = current.getPairEnumeration(hashCode);

		Pair<K, V> targetPair = null;
		Pair<K, V> previousPair = null;
		Pair<K, V> nextPair = null;

		while (pairEnum.hasMoreElements()) {
			Pair<K, V> pair = pairEnum.nextElement();
			try {
				if (pair.getKey().equals(key)) {
					targetPair = pair;
					break;
				}
			} catch (IOException | ClassNotFoundException e) {
				throw new PMapException("Unable to read data.", e);
			}
			previousPair = pair;
		}

		if (targetPair == null)
			// no entry found
			return null;

		// At this point a value exists and has to be removed /
		// unreferenced.

		if (pairEnum.hasMoreElements()) {
			nextPair = pairEnum.nextElement();
		}

		long nextPairPointer = PMap.EOF_POSITION;
		if (nextPair != null) {
			nextPairPointer = nextPair.getPairPointer();
		}

		boolean pairLessNode = false;
		if (previousPair != null) {
			previousPair.setNextPairPointer(nextPairPointer);
			previousPair.write(getPairFileDriver());
		} else {
			// write into the index

			pairLessNode = nextPairPointer == PMap.EOF_POSITION;

			if (!pairLessNode) {
				current.setNextPairPointer(hashCode, nextPairPointer);
				current.write(pMap.getIndexFileDriver());
			}
		}

		registerFreePairPosition(targetPair);

		return new PairTuple<K, V>(pairLessNode, targetPair);
	}

	/**
	 * Registers the space occupied by this removed {@link Pair} for further needs
	 * 
	 * 
	 * @param removedPair
	 * @throws IOException
	 */
	private void registerFreePairPosition(Pair<K, V> removedPair) throws IOException {
		 pairFactory.registerFreePairPosition(removedPair);
	}

	/**
	 * Retrieves and sets inside the new {@link Pair} free space occupied by previously removed {@link Pair}s
	 * 
	 * @param newPair
	 * @throws IOException
	 */
	private void poolFreePairPosition(Pair<K, V> newPair) throws IOException {
		 pairFactory.poolFreePairPosition(newPair);
		
	}

	

}