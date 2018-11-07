package com.pmaps.pmap.pair;

import java.io.IOException;
import java.util.Enumeration;
import java.util.NoSuchElementException;

import com.pmaps.PMapException;
import com.pmaps.pmap.PMap;

public class PairEnumeration<K, V> implements Enumeration<Pair<K, V>> {

	private long nextPairPointer;
	private final PMap<K, V> pMap;

	public PairEnumeration(PMap<K, V> pMap, final long nextPairPointer) {
		this.pMap = pMap;
		this.nextPairPointer = nextPairPointer;
	}

	@Override
	public boolean hasMoreElements() {
		return nextPairPointer != PMap.EOF_POSITION;
	}

	@Override
	public Pair<K, V> nextElement() {
		if (nextPairPointer != PMap.EOF_POSITION) {
			try {
				Pair<K, V> pair = pMap.getPairDriver().getPairFactory().newPairForReading(pMap, nextPairPointer);
				// new PlainPair<K, V>(pMap, nextPairPointer);
				nextPairPointer = pair.getNextPairPointer();
				return pair;
			} catch (IOException e) {
				throw new PMapException("Unable to retrieve the next element", e);
			}
		}
		throw new NoSuchElementException();
	}

}
