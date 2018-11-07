package com.pmaps.pmap.set;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.Iterator;

import com.pmaps.PMapException;
import com.pmaps.pmap.PMap;

public class ValueSet<V> extends AbstractSet<V > {

	/**
	 * 
	 */
	private final PMap<?,V> pMap;

	/**
	 * @param pMap
	 */
	public ValueSet(PMap<?, V> pMap) {
		this.pMap = pMap;
	}

	@Override
	public Iterator<V> iterator() {
		try {
			return new ValueIterator<V>(pMap);
		} catch (IOException e) {
			throw new PMapException("Unable to create the iterator", e);
		}
	}

	@Override
	public int size() {
		return pMap.size();
	}

}